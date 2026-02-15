import os
import sys
import time
import socket
import requests
import websocket
import traceback


PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
sys.path.insert(0, PROJECT_ROOT)

from src.logger import logger
from src.utils.csv_ops import CsvOps
from src.utils.market_ops import MarketOps
from src.clients.capitap_client import CapitalClient
from src.utils.candle_aggregator import CandleAggregator

from src.stretegies.yesterday_high_low import YesterdayHighLowStrategy


def main():
    try:

        print(f"API Key: {os.getenv('CAPITAL_DEMO_API_KEY')}")
        print(f"Account ID: {os.getenv('CAPITAL_IDENTIFIER')}")
        print(f"Account PWD: {os.getenv('CAPITAL_PASSWORD')}")

        logger.info("=" * 40)
        print("Algorithmic Trading Bot Started")

        ### ======= Initialize variables ======= ###
        client = CapitalClient(
            api_key=os.getenv("CAPITAL_DEMO_API_KEY"),
            identifier=os.getenv("CAPITAL_IDENTIFIER"),
            password=os.getenv("CAPITAL_PASSWORD"),
        )

        market_ops = MarketOps(client)

        resolution = "MINUTE"
        candle_aggregator = CandleAggregator(resolution=resolution)

        ### ======= Login ======= ###
        client.login()

        ### ======= Fetch Epic ======= ###
        markets = client.search_markets("BTC")
        if markets:
            epic_to_use = markets[0]["epic"]
            logger.info(f"Using epic: {epic_to_use}")

        else:
            epic_to_use = "CS.D.EURUSD.MINI.IP"  # Fallback
            logger.info(f"No markets found, using: {epic_to_use}")

        ### ======= Fetch Account Info ======= ###
        all_accounts = client.get_accounts()
        account_info = next(
            (
                account
                for account in all_accounts
                if account.get("accountId") == os.getenv("DEMO_ACCOUNT_ID")
            ),
            None,
        )

        if account_info is None:
            logger.info(f" Account with ID {os.getenv('DEMO_ACCOUNT_ID')} not found.")
            balance = 0
            available_balance = 0

        balance = account_info.get("balance", {}).get("balance", 0)
        available_balance = account_info.get("balance", {}).get("available", 0)

        logger.info(
            f"Account Balance: {balance}, Available balance: {available_balance}"
        )

        ### ======= Update Yesterday's Levels ======= ###
        yesterday_levels = market_ops.update_yesterday_levels(
            epic=epic_to_use,
            resolution=resolution,
            csv_path=f"data/{epic_to_use}_yesterday_levels.csv",
        )
        logger.info(f"Yesterday's Levels:\n{yesterday_levels}")

        ### ======= Start WebSocket Stream ======= ###
        strategy = YesterdayHighLowStrategy(
            epic=epic_to_use,
            account_balance=available_balance,
        )

        def on_tick_received(tick: dict):
            closed_candle = candle_aggregator.process_tick(tick)

            if closed_candle:

                signal = (
                    strategy.on_candle_close(closed_candle) if closed_candle else None
                )

                if signal and signal["decision"] == "SIGNAL":
                    logger.info(f"Signal detected: {signal}")

                    order = signal["order"]

                    response = client.create_working_order(
                        epic=order["epic"],
                        direction=order["direction"],
                        size=order["size"],
                        level=order["level"],  # C3 price
                        order_type=order["orderType"],  # STOP
                        stop_level=order["stopLevel"],
                        profit_level=order["profitLevel"],
                        trailing_stop=False,
                        guaranteed_stop=False,
                    )

                    # --- BUILD TRADE RECORD ---
                    trade_row = {
                        "trade_date": signal["time"].date(),
                        "trade_time": signal["time"],
                        "epic": order["epic"],
                        "direction": order["direction"],
                        "entry_price": order["level"],
                        "stop_loss": order["stopLevel"],
                        "take_profit": order["profitLevel"],
                        "position_size": order["size"],
                        "risk_percent": strategy.risk_percent * 100,
                        "account_balance": strategy.account_balance,
                        "yesterday_high": strategy.y_high,
                        "yesterday_low": strategy.y_low,
                        "c1_time": strategy.c1["start_time"],
                        "c2_time": strategy.c2["start_time"],
                        "order_type": order["orderType"],
                        "deal_id": response.get("dealId"),
                        "order_id": response.get("order_id"),
                        "deal_reference": response.get("dealReference"),
                        "strategy_name": "YesterdayHighLow",
                        "status": "EXECUTED",
                    }

                    # Write trade record to CSV
                    csv_ops = CsvOps(path="data/trade_book.csv")
                    csv_ops.append_row(trade_row)

        ws = client.stream_ticks(
            epics=[epic_to_use],
            on_tick=on_tick_received,
            auto_reconnect=True,
            reconnect_delay=2,
        )

        # Keep main thread alive with minimal overhead
        while True:
            time.sleep(0.001)  # 1ms

    # --- USER ACTION
    except KeyboardInterrupt:
        logger.info("User stopped the bot (Ctrl+C)")

    # --- AUTH / SESSION / API ERRORS
    except requests.exceptions.HTTPError as e:
        status = e.response.status_code if e.response else "UNKNOWN"
        body = e.response.text if e.response else "NO RESPONSE"
        logger.error(f"HTTP error {status}: {body}")
        logger.debug(traceback.format_exc())

    except requests.exceptions.ConnectionError:
        logger.error("Network connection error (API unreachable)")
        logger.debug(traceback.format_exc())

    except requests.exceptions.Timeout:
        logger.error("API request timed out")
        logger.debug(traceback.format_exc())

    # --- WEBSOCKET ERRORS
    except websocket.WebSocketException as e:
        logger.error(f"WebSocket error: {e}")
        logger.debug(traceback.format_exc())

    except socket.gaierror:
        logger.error("DNS resolution failed (network / internet issue)")
        logger.debug(traceback.format_exc())

    # --- STRATEGY / LOGIC ERRORS
    except ValueError as e:
        logger.error(f"Strategy / validation error: {e}")
        logger.debug(traceback.format_exc())

    except KeyError as e:
        logger.error(f"Missing expected field: {e}")
        logger.debug(traceback.format_exc())

    # --- MEMORY / PERFORMANCE
    except MemoryError:
        logger.critical("Out of memory — shutting down immediately")

    #  --- EVERYTHING ELSE (LAST RESORT)
    except Exception:
        logger.critical("Unhandled fatal exception")
        logger.critical(traceback.format_exc())

    # -- CLEAN UP
    finally:
        if client:
            try:
                client.stop_streaming()
            except Exception:
                logger.warning("Error while stopping WebSocket")

        logger.warning("Trading bot stopped cleanly")


if __name__ == "__main__":
    main()
