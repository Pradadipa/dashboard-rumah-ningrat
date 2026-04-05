# =============================================================================
# connectors/csv_connector.py
# -----------------------------------------------------------------------------
# Handles loading raw data from CSV/Excel files.
# Used for:
#   - Local development with sample data
#   - Fallback if Meta API is unavailable
#   - Testing the dashboard without API credentials
#
# All functions return a raw pandas DataFrame — no transformation here.
# Cleaning and normalization happens in data/transformer.py
# =============================================================================

import pandas as pd
from pathlib import Path
from typing import Optional

from config.settings import SAMPLE_DIR, DATE_FORMAT
from config.constants import FUNNEL_STAGES, FILE_MAP

# -----------------------------------------------------------------------------
# LOAD FROM FILE PATH
# Core function — reads a single CSV or Excel file into a DataFrame.
# -----------------------------------------------------------------------------
def load_file(file_path: str | Path) -> pd.DataFrame:
    """
    Load a CSV or Excel file from the given path.

    Args:
        file_path: Absolute or relative path to the file.

    Returns:
        Raw pandas DataFrame with original column names from the file.

    Raises:
        FileNotFoundError: If the file does not exist.
        ValueError: If the file format is not supported.
    """
    path = Path(file_path)

    if not path.exists():
        raise FileNotFoundError(f"Data file not found:{path}")
    
    suffix = path.suffix.lower()

    if suffix == ".csv":
        df = pd.read_csv(path)
    elif suffix in [".xlsx", ".xls"]:
        df = pd.read_excel(path, engine="openpyxl")
    else:
        raise ValueError(f"Unsupported file format: '{suffix}'. Use .csv or .xlsx")
    
    # Strip whitespace from column names (common issue in exports)
    df.columns = df.columns.str.strip()

    return df

# -----------------------------------------------------------------------------
# LOAD SAMPLE DATA BY FUNNEL STAGE
# Convenience function that loads the pre-defined sample file
# for a given funnel stage using FILE_MAP from constants.py
# -----------------------------------------------------------------------------
def load_sample_data(stage: str) -> pd.DataFrame:
    """
    Load sample CSV data for a given funnel stage.

    Args:
        stage: One of "prospecting", "retention", or "awareness"

    Returns:
        Raw pandas DataFrame from the sample file.

    Raises:
        ValueError: If stage is not a valid funnel stage.
        FileNotFoundError: If the sample file does not exist.
    """
    if stage not in FUNNEL_STAGES:
        raise ValueError(
            f"Invalid funnel stage: '{stage}'. "
            f"Must be one of: {FUNNEL_STAGES}"
        )

    file_path = Path(FILE_MAP[stage])
    return load_file(file_path)

# -----------------------------------------------------------------------------
# LOAD WITH DATE FILTER
# Loads a file and immediately filters by date range.
# Used when working with large CSV files covering many months.
# -----------------------------------------------------------------------------
def load_file_with_date_filter(
        file_path: str | Path,
        start_date: str,
        end_date: str,
        date_column: str = "date"
) -> pd.DataFrame:
    """
    Load a CSV file and filter rows within the given date range.

    Args:
        file_path:   Path to the CSV or Excel file.
        start_date:  Start date string in YYYY-MM-DD format.
        end_date:    End date string in YYYY-MM-DD format.
        date_column: Name of the date column in the file (default: "date").

    Returns:
        Filtered pandas DataFrame containing only rows in the date range.
    """
    df = load_file(file_path)

    if date_column not in df.columns:
        raise KeyError(
            f"Date column '{date_column}' not found in file. "
            f"Available columns: {df.columns}"
        )
    # Parse date column -  handle mixed formats and ensure it's in datetime format
    df[date_column] = pd.to_datetime(df[date_column], format=DATE_FORMAT, errors='coerce')

    # Warn about any rows that couldn't be parsed as dates
    null_dates = df[date_column].isnull().sum()
    if null_dates > 0:
        print(f"⚠️  Warning: {null_dates} rows had unparseable dates and will be excluded.")
    
    # Apply date range filter
    start = pd.to_datetime(start_date, format=DATE_FORMAT)
    end = pd.to_datetime(end_date, format=DATE_FORMAT)
    mask = (df[date_column] >= start) & (df[date_column] <= end)

    return df.loc[mask].reset_index(drop=True)

# -----------------------------------------------------------------------------
# VALIDATE CSV STRUCTURE
# Checks that a loaded DataFrame has the expected columns.
# Called by loader.py before passing data to transformer.py
# -----------------------------------------------------------------------------
def validate_columns(
        df: pd.DataFrame,
        required_columns: list[str],
        source_name: str = 'file'
) -> list[str]:
    """
    Validate that a DataFrame contains all required columns.

    Args:
        df:               DataFrame to validate.
        required_columns: List of column names that must be present.
        source_name:      Label used in warning messages (e.g. filename).

    Returns:
        List of missing column names (empty list if all present).
    """
    missing = [col for col in required_columns if col not in df.columns]

    if missing:
        print(f"⚠️  Warning: The following required columns are missing from {source_name}: {missing}")
    
    return missing

# -----------------------------------------------------------------------------
# GENERATE SAMPLE CSV FILES
# Creates realistic sample data files for development and testing.
# Run this once to populate data/sample/ before the API is connected.
# -----------------------------------------------------------------------------
def generate_sample_files() -> None:
    """
    Generate sample CSV files for all funnel stages.
    Saves files to the SAMPLE_DIR defined in settings.py.

    Run from the project root:
        python -c "from connectors.csv_connector import generate_sample_files; generate_sample_files()"
    """
    import numpy as np

    SAMPLE_DIR.mkdir(parents=True, exist_ok=True)

    # Shared date range for all sample files
    dates = pd.date_range(end=pd.Timestamp.today(), periods=30, freq="D")
    np.random.seed(42)   # reproducible sample data

    # -----------------------------------------------------------------
    # Prospecting & Retention sample (same schema)
    # -----------------------------------------------------------------
    for stage in ["prospecting", "retention"]:
        n_ads = 6
        records = []

        for i in range(1, n_ads + 1):
            for date in dates:
                spend         = round(np.random.uniform(10, 200), 2)
                clicks        = np.random.randint(20, 500)
                purchases     = np.random.randint(0, 20)
                purchase_value= round(purchases * np.random.uniform(30, 150), 2)

                records.append({
                    "ad_name":       f"{stage.capitalize()} Ad {i}",
                    "date":          date.strftime(DATE_FORMAT),
                    "spend":         spend,
                    "clicks":        clicks,
                    "purchases":     purchases,
                    "purchase_value":purchase_value,
                    "impressions":   np.random.randint(500, 10000),
                })

        df = pd.DataFrame(records)
        output_path = SAMPLE_DIR / f"{stage}.csv"
        df.to_csv(output_path, index=False)
        print(f"✅ Generated: {output_path} ({len(df)} rows)")

    # -----------------------------------------------------------------
    # Awareness sample (different schema)
    # -----------------------------------------------------------------
    n_ads = 6
    records = []

    for i in range(1, n_ads + 1):
        for date in dates:
            spend = round(np.random.uniform(10, 200), 2)

            records.append({
                "ad_name":        f"Awareness Ad {i}",
                "date":           date.strftime(DATE_FORMAT),
                "spend":          spend,
                "impressions":    np.random.randint(1000, 50000),
                "post_engagement":np.random.randint(10, 500),
            })

    df = pd.DataFrame(records)
    output_path = SAMPLE_DIR / "awareness.csv"
    df.to_csv(output_path, index=False)
    print(f"✅ Generated: {output_path} ({len(df)} rows)")
