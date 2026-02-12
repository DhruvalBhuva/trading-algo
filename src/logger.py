import os
import logging
from datetime import datetime

# Generate the log file name based on the current date
LOG_FILE = f"{datetime.now().strftime('%d_%m_%Y')}.log"
logs_dir = os.path.join(os.getcwd(), "logs")
os.makedirs(logs_dir, exist_ok=True)

LOG_FILE_PATH = os.path.join(logs_dir, LOG_FILE)

# Configure logging to append to the daily log file
logging.basicConfig(
    filename=LOG_FILE_PATH,
    format="[ %(asctime)s ] %(filename)s:%(lineno)d %(name)s - %(levelname)s - %(message)s",
    level=logging.INFO,
)

# Get the logger instance
logging.getLogger().setLevel(logging.WARNING)
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# Test the logging
if __name__ == "__main__":
    logger.info("This is a test log message")
    logger.warning("This is a test warning message")
    logger.error("This is a test error message")
    logger.critical("This is a test critical message")
    logger.debug("This is a test debug message (should not appear in INFO level)")
