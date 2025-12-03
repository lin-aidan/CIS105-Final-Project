"""
csvs_to_sqlite.py

Load specified CSV files into a SQLite database. Each CSV becomes its own table.
Table names use the CSV filename without the `.csv` extension.

Usage:
    python csvs_to_sqlite.py

The script will create `data/rb_analysis.db` and add tables:
 - rb_vs_top
 - rb_bottom
 - defense_stats

"""
import os
import sqlite3
import logging
import pandas as pd

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s: %(message)s')
logger = logging.getLogger(__name__)

DB_PATH = 'data/rb_analysis.db'
CSV_FILES = [
    'data/rb_vs_top.csv',
    'data/rb_bottom.csv',
    'defense_stats.csv',
]


def csv_to_table_name(path: str) -> str:
    base = os.path.basename(path)
    name, ext = os.path.splitext(base)
    # Use the base filename (without extension) as table name
    # Make safe: replace spaces and hyphens with underscore, lower-case
    tbl = name.replace(' ', '_').replace('-', '_').lower()
    return tbl


def main():
    os.makedirs('data', exist_ok=True)

    # Remove existing DB if present (replace behavior)
    if os.path.exists(DB_PATH):
        logger.info('Removing existing DB at %s', DB_PATH)
        os.remove(DB_PATH)

    conn = sqlite3.connect(DB_PATH)

    for csv in CSV_FILES:
        if not os.path.exists(csv):
            logger.warning('CSV file not found, skipping: %s', csv)
            continue

        table = csv_to_table_name(csv)
        logger.info('Loading %s into table `%s`', csv, table)

        try:
            df = pd.read_csv(csv)
        except Exception as e:
            logger.error('Failed to read %s: %s', csv, e)
            continue

        # Normalize column names: replace spaces with underscores
        df.columns = [c.strip().replace(' ', '_').replace('.', '').replace('(', '').replace(')', '') for c in df.columns]

        try:
            df.to_sql(table, conn, if_exists='replace', index=False)
            logger.info('Wrote %d rows to %s.%s', len(df), DB_PATH, table)
        except Exception as e:
            logger.error('Failed to write table %s: %s', table, e)

    conn.close()
    logger.info('Database created at %s', DB_PATH)


if __name__ == '__main__':
    main()
