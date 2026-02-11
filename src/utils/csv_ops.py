import csv
from pathlib import Path
from typing import Iterable, Dict, List, Optional

import pandas as pd


class CsvOpsError(Exception):
    """Base class for CSV operation errors."""


class CsvSchemaError(CsvOpsError):
    """Raised when CSV schema mismatches."""


class CsvIOError(CsvOpsError):
    """Raised when file I/O fails."""


class CsvOps:
    """
    Generic CSV utility for reading & writing structured data.

    Features:
    - Auto-detect schema from existing CSV
    - Schema enforced on append
    - Auto-create directories
    - Datetime normalization
    """

    def __init__(self, path: str | Path, schema: Optional[List[str]] = None):
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)

        self.schema: Optional[List[str]] = None

        if self.path.exists():
            self.schema = self._load_schema_from_file()
        else:
            if schema is None:
                raise CsvSchemaError(
                    f"CSV does not exist and no schema provided: {self.path}"
                )
            self.schema = list(schema)

    # -------------------------------------------------
    # Schema
    # -------------------------------------------------
    def _load_schema_from_file(self) -> List[str]:
        try:
            df = pd.read_csv(self.path, nrows=0)
            return list(df.columns)
        except Exception as e:
            raise CsvIOError(f"Failed to read CSV schema: {self.path}") from e

    # -------------------------------------------------
    # Normalization
    # -------------------------------------------------
    @staticmethod
    def _normalize_row(row: Dict) -> Dict:
        normalized = {}
        for k, v in row.items():
            if hasattr(v, "isoformat"):
                normalized[k] = v.isoformat()
            else:
                normalized[k] = v
        return normalized

    # -------------------------------------------------
    # Write Operations
    # -------------------------------------------------
    def append_row(self, row: Dict):
        """
        Append a single row to CSV with strict schema enforcement.
        """
        if not self.schema:
            raise CsvSchemaError("CSV schema is not defined")

        row = self._normalize_row(row)

        missing = set(self.schema) - row.keys()
        extra = row.keys() - set(self.schema)

        if missing or extra:
            raise CsvSchemaError(
                f"Row schema mismatch for {self.path}\n"
                f"Missing: {missing}\n"
                f"Extra:   {extra}"
            )

        try:
            write_header = not self.path.exists()

            with self.path.open("a", newline="", encoding="utf-8") as f:
                writer = csv.DictWriter(f, fieldnames=self.schema)

                if write_header:
                    writer.writeheader()

                writer.writerow(row)

        except Exception as e:
            raise CsvIOError(f"Failed to append row to {self.path}") from e

    def append_rows(self, rows: Iterable[Dict]):
        rows = list(rows)
        if not rows:
            return

        for row in rows:
            self.append_row(row)

    # -------------------------------------------------
    # Read Operations
    # -------------------------------------------------
    def read(self) -> pd.DataFrame:
        try:
            return pd.read_csv(self.path)
        except FileNotFoundError:
            return pd.DataFrame(columns=self.schema)
        except Exception as e:
            raise CsvIOError(f"Failed to read CSV: {self.path}") from e

    def read_tail(self, n: int = 1) -> pd.DataFrame:
        try:
            return pd.read_csv(self.path).tail(n)
        except FileNotFoundError:
            return pd.DataFrame(columns=self.schema)
        except Exception as e:
            raise CsvIOError(f"Failed to read tail of CSV: {self.path}") from e

    # -------------------------------------------------
    # Maintenance
    # -------------------------------------------------
    def exists(self) -> bool:
        return self.path.exists()

    def delete(self):
        try:
            if self.path.exists():
                self.path.unlink()
        except Exception as e:
            raise CsvIOError(f"Failed to delete CSV: {self.path}") from e

    def backup(self, suffix: str = ".bak"):
        try:
            backup_path = self.path.with_suffix(self.path.suffix + suffix)
            self.path.replace(backup_path)
        except Exception as e:
            raise CsvIOError(f"Failed to backup CSV: {self.path}") from e
