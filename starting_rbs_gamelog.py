"""
starting_rbs_gamelog.py

Fetches game logs for the 32 starting NFL running backs from ESPN.
Reads player IDs from data/starting_rbs.csv and scrapes their individual game logs.
Saves combined results to data/starting_rbs_gamelog.csv.

Usage:
    pip install requests beautifulsoup4 pandas
    python starting_rbs_gamelog.py

Notes:
- Uses ESPN player gamelog pages: https://www.espn.com/nfl/player/gamelog/_/id/{player_id}
- Rate-limits requests with time.sleep to avoid overloading ESPN.
- Skips a player if any error occurs while fetching/parsing.
"""

import os
import time
import logging
from typing import Dict, List

import requests
from bs4 import BeautifulSoup
import pandas as pd

# Logging setup
logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s: %(message)s')
logger = logging.getLogger(__name__)

# ESPN user-agent
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
}

# Paths
INPUT_CSV = 'data/starting_rbs.csv'
OUTPUT_DIR = 'data'
OUTPUT_CSV = os.path.join(OUTPUT_DIR, 'starting_rbs_gamelog.csv')


def fetch_player_gamelog(player_id: int, player_name: str, team: str, session: requests.Session) -> List[Dict]:
    """
    Fetch game log for a specific player from ESPN gamelog page.
    Returns a list of dicts with game statistics.
    Format: Date, Opponent, Result, [Rushing: CAR, YDS, AVG, TD, LNG], [Receiving: REC, TGTS, YDS, ...]
    """
    url = f'https://www.espn.com/nfl/player/gamelog/_/id/{player_id}'
    logger.info('Fetching game log for %s (ID: %s)', player_name, player_id)

    try:
        resp = session.get(url, headers=HEADERS, timeout=10)
        resp.raise_for_status()
    except requests.exceptions.RequestException as e:
        logger.error('Request error for %s (%s): %s', player_name, player_id, e)
        return []

    soup = BeautifulSoup(resp.content, 'html.parser')
    games = []

    try:
        # Find the game log table (first table on the page)
        tables = soup.find_all('table')
        
        if not tables:
            logger.warning('No tables found for %s', player_name)
            return []

        table = tables[0]
        rows = table.find_all('tr')

        # Skip header rows (first 2 rows are headers with season info and column labels)
        for row in rows[2:]:
            cells = row.find_all('td')
            if len(cells) < 8:  # Need at least: Date, Opp, Result, CAR, YDS, AVG, TD, LNG
                continue

            try:
                cell_texts = [cell.get_text(strip=True) for cell in cells]

                # Extract game info
                # Row format: Date, OPP, Result, CAR, YDS, AVG, TD, LNG, REC, TGTS, YDS, ...
                game_data = {
                    'player_id': player_id,
                    'player_name': player_name,
                    'team': team,
                    'date': cell_texts[0],
                    'opponent': cell_texts[1],
                    'result': cell_texts[2],
                }

                # Rushing stats (columns 3-7: CAR, YDS, AVG, TD, LNG)
                if len(cell_texts) > 6:
                    try:
                        game_data['rushing_attempts'] = int(cell_texts[3]) if cell_texts[3] else None
                        game_data['rushing_yards'] = int(cell_texts[4]) if cell_texts[4] else None
                        game_data['rushing_avg'] = float(cell_texts[5]) if cell_texts[5] else None
                        game_data['rushing_td'] = int(cell_texts[6]) if cell_texts[6] else None
                        game_data['rushing_lng'] = int(cell_texts[7]) if cell_texts[7] and cell_texts[7].isdigit() else None
                    except (ValueError, IndexError) as e:
                        logger.debug('Error parsing rushing stats for %s: %s', player_name, e)

                # Receiving stats (columns 8+: REC, TGTS, YDS, AVG, TD, LNG)
                if len(cell_texts) > 13:
                    try:
                        game_data['receiving_receptions'] = int(cell_texts[8]) if cell_texts[8] else None
                        game_data['receiving_targets'] = int(cell_texts[9]) if cell_texts[9] else None
                        game_data['receiving_yards'] = int(cell_texts[10]) if cell_texts[10] else None
                    except (ValueError, IndexError) as e:
                        logger.debug('Error parsing receiving stats for %s: %s', player_name, e)

                games.append(game_data)

            except Exception as e:
                logger.debug('Error parsing game row for %s: %s', player_name, e)
                continue

        logger.info('Found %d games for %s', len(games), player_name)

    except Exception as e:
        logger.error('Error processing game log page for %s: %s', player_name, e)
        return []

    return games


def scrape_all_starting_rb_gamelogs(input_csv: str = INPUT_CSV) -> List[Dict]:
    """
    Read starting RBs from CSV and fetch game logs for each.
    Returns combined list of game entries.
    """
    # Read starting RBs
    try:
        df = pd.read_csv(input_csv)
        logger.info('Loaded %d starting RBs from %s', len(df), input_csv)
    except Exception as e:
        logger.error('Failed to read %s: %s', input_csv, e)
        return []

    session = requests.Session()
    all_games = []

    for idx, row in df.iterrows():
        player_id = row['player_id']
        player_name = row['player_name']
        team = row['team']

        games = fetch_player_gamelog(player_id, player_name, team, session)
        all_games.extend(games)

        # Rate limiting: sleep 1 second between requests
        time.sleep(1.0)

    logger.info('Total games collected: %d', len(all_games))
    return all_games


def main():
    """Main entry point."""
    logger.info('Starting starting_rbs_gamelog scraper')

    # Ensure output directory exists
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    # Scrape all game logs
    all_games = scrape_all_starting_rb_gamelogs()

    if not all_games:
        logger.warning('No games collected.')
        return

    # Convert to DataFrame and save
    df = pd.DataFrame(all_games)
    df.to_csv(OUTPUT_CSV, index=False)
    logger.info('Saved %d game entries to %s', len(df), OUTPUT_CSV)

    # Print sample
    logger.info('Sample of data:\n%s', df.head(10))


if __name__ == '__main__':
    main()
