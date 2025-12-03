"""
starting_rbs.py

Scrape ESPN depth chart pages to determine the starting running back (RB1)
for each NFL team and save results to `data/starting_rbs.csv`.

Notes:
- Uses `requests` + `BeautifulSoup`.
- Sleeps 1 second between requests to be polite.
- Includes basic error handling; skips teams that fail.

Run:
    pip install requests beautifulsoup4 pandas
    python starting_rbs.py
"""

import os
import re
import time
import logging
from typing import List, Dict, Optional

import requests
from bs4 import BeautifulSoup
import pandas as pd

# Logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s: %(message)s')
logger = logging.getLogger(__name__)

# ESPN user-agent
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
}

# Team abbreviations used by ESPN in URLs
TEAM_ABBREVS = [
    'nyg','cin','buf','mia','chi','wsh','atl','ten','min','nyj','car','dal',
    'no','bal','phi','ari','lac','sf','pit','lv','lar','cle','det','kc','tb',
    'ind','gb','ne','hou','sea','den','jax'
]

DEPTH_URL = 'https://www.espn.com/nfl/team/depth/_/name/{team_abbrev}'
OUTPUT_DIR = 'data'
OUTPUT_CSV = os.path.join(OUTPUT_DIR, 'starting_rbs.csv')

PLAYER_ID_RE = re.compile(r'/player/_/id/(\d+)')

SLEEP_SECONDS = 1.0


def extract_player_from_link(a_tag) -> Optional[Dict[str,str]]:
    """Given an <a> tag to a player profile, extract name and player_id."""
    if not a_tag:
        return None
    href = a_tag.get('href', '')
    m = PLAYER_ID_RE.search(href)
    if not m:
        return None
    player_id = m.group(1)
    name = a_tag.get_text(strip=True)
    if not name:
        return None
    return {'player_name': name, 'player_id': player_id}


def extract_starting_rb_from_depth_table(soup: BeautifulSoup) -> Optional[Dict[str, str]]:
    """Extract the starting RB from ESPN's depth chart table structure.

    ESPN depth charts use a two-table layout:
    - Table 0: Position labels (QB, RB, WR, etc.)
    - Table 1: Player names and links

    We find the RB row in Table 0, then extract the first player link from
    the corresponding row in Table 1.

    Returns dict with player_name and player_id, or None if not found.
    """
    depth_table = soup.find('div', class_='nfl-depth-table')
    if not depth_table:
        return None

    tables = depth_table.find_all('table')
    if len(tables) < 2:
        return None

    pos_table = tables[0]  # Positions
    player_table = tables[1]  # Players

    # Extract rows (skip header)
    pos_rows = pos_table.find_all('tr')[1:]
    player_rows = player_table.find_all('tr')[1:]

    if len(pos_rows) != len(player_rows):
        logger.debug('Position and player row counts mismatch')

    # Find which row is the RB
    rb_row_idx = None
    for i, row in enumerate(pos_rows):
        text = row.get_text(strip=True)
        if 'RB' in text:
            rb_row_idx = i
            break

    if rb_row_idx is None:
        logger.debug('RB position not found in depth chart')
        return None

    # Get the corresponding player row
    if rb_row_idx >= len(player_rows):
        logger.debug('RB row index out of range for player table')
        return None

    player_row = player_rows[rb_row_idx]

    # Extract the first player link (the starter)
    a_tag = player_row.find('a', href=PLAYER_ID_RE)
    if not a_tag:
        logger.debug('No player link in RB row')
        return None

    player = extract_player_from_link(a_tag)
    return player


def get_starting_rb_for_team(team_abbrev: str, session: requests.Session) -> Optional[Dict]:
    """Fetch depth chart page and extract the first RB (RB1) if available."""
    url = DEPTH_URL.format(team_abbrev=team_abbrev)
    logger.info('Fetching depth chart for %s -> %s', team_abbrev, url)

    try:
        resp = session.get(url, headers=HEADERS, timeout=10)
        resp.raise_for_status()
    except requests.exceptions.RequestException as e:
        logger.error('Request error for %s: %s', team_abbrev, e)
        return None

    soup = BeautifulSoup(resp.content, 'html.parser')

    # Extract RB from depth table
    player = extract_starting_rb_from_depth_table(soup)

    if not player:
        logger.warning('Could not extract RB for %s', team_abbrev)
        return None

    # Build result
    return {
        'team': team_abbrev,
        'player_name': player['player_name'],
        'player_id': player['player_id'],
        'depth_rank': 'RB1'
    }


def scrape_all_starting_rbs(team_abbrevs: List[str] = None) -> pd.DataFrame:
    team_abbrevs = team_abbrevs or TEAM_ABBREVS
    session = requests.Session()

    rows = []
    for team in team_abbrevs:
        try:
            res = get_starting_rb_for_team(team, session)
            if res:
                rows.append(res)
                logger.info('Found RB1 for %s: %s (%s)', team, res['player_name'], res['player_id'])
            else:
                logger.info('No RB1 found for %s', team)
        except requests.exceptions.RequestException as e:
            logger.error('Request error for %s: %s', team, e)
        except Exception as e:
            logger.exception('Error extracting RB1 for %s: %s', team, e)

        # rate limit
        time.sleep(SLEEP_SECONDS)

    df = pd.DataFrame(rows, columns=['team', 'player_name', 'player_id', 'depth_rank'])
    return df


def ensure_output_dir():
    if not os.path.isdir(OUTPUT_DIR):
        os.makedirs(OUTPUT_DIR, exist_ok=True)


if __name__ == '__main__':
    logger.info('Starting starting_rbs scraper for %d teams', len(TEAM_ABBREVS))
    df = scrape_all_starting_rbs()
    if df.empty:
        logger.warning('No starting RBs found; nothing to save')
    else:
        ensure_output_dir()
        df.to_csv(OUTPUT_CSV, index=False)
        logger.info('Saved %d starting RBs to %s', len(df), OUTPUT_CSV)
