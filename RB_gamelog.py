"""
RB_gamelog.py

Scrapes ESPN team roster pages to find Running Backs (RB) for each NFL team.
Saves results to `data/rb_player_ids.csv`.

Usage:
    pip install requests beautifulsoup4 pandas
    python RB_gamelog.py

Notes:
- Uses `requests` + `BeautifulSoup`.
- Rate-limits requests with `time.sleep` to avoid hitting ESPN too hard.
- Skips a team if any error occurs while fetching/parsing.
"""

import os
import re
import time
import random
import logging
from typing import List, Dict

import requests
from bs4 import BeautifulSoup
import pandas as pd

# Basic logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s: %(message)s')
logger = logging.getLogger(__name__)

# ESPN user-agent
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
}

# ESPN team abbreviations (used in roster URLs). These are the values ESPN uses in URLs, lowercase.
TEAM_ABBREVS = [
    'nyg','cin','buf','mia','chi','wsh','atl','ten','min','nyj','car','dal',
    'no','bal','phi','ari','lac','sf','pit','lv','lar','cle','det','kc','tb',
    'ind','gb','ne','hou','sea','den','jax'
]

# Roster URL template
ROSTER_URL = 'https://www.espn.com/nfl/team/roster/_/name/{team_abbrev}'

# Output path
OUTPUT_DIR = 'data'
OUTPUT_CSV = os.path.join(OUTPUT_DIR, 'rb_player_ids.csv')

# Regex to find ESPN player id in player profile URLs
PLAYER_ID_RE = re.compile(r'/player/_/id/(\d+)')

# Rate limiting settings
MIN_SLEEP = 1.0
MAX_SLEEP = 2.0


def fetch_team_roster(team_abbrev: str, session: requests.Session = None) -> List[Dict]:
    """Fetch roster page for a team and extract RBs.

    Returns a list of dicts with keys: team (abbrev), player_name, player_id
    """
    session = session or requests.Session()
    url = ROSTER_URL.format(team_abbrev=team_abbrev)

    logger.info('Fetching roster for %s: %s', team_abbrev, url)
    resp = session.get(url, headers=HEADERS, timeout=10)
    resp.raise_for_status()

    soup = BeautifulSoup(resp.content, 'html.parser')

    results = []
    seen_ids = set()

    # Strategy: Find all links to player profiles, then for each link try to find
    # the position from the parent row (<tr>) or nearby cells. Include player
    # when position contains 'RB'.
    for a in soup.find_all('a', href=re.compile(r'/player/_/id/\d+')):
        href = a.get('href', '')
        match = PLAYER_ID_RE.search(href)
        if not match:
            continue
        player_id = match.group(1)
        player_name = a.get_text(strip=True)
        if not player_name:
            continue

        # Avoid duplicates
        if player_id in seen_ids:
            continue

        # Try to determine position text from parent <tr> if present
        position_text = ''
        parent_tr = a.find_parent('tr')

        if parent_tr:
            # Collect text from all <td> in row (positions are often in a column)
            tds = parent_tr.find_all('td')
            for td in tds:
                text = td.get_text(separator=' ', strip=True)
                if text:
                    # Check if this td looks like a position cell (single short code)
                    # Many ESPN roster tables put position in a cell like 'RB', 'WR', etc.
                    if re.search(r'\bRB\b', text, flags=re.IGNORECASE):
                        position_text = text
                        break
                    # Otherwise keep the last short token as fallback
                    # (but only if it's short)
                    tokens = text.split()
                    if tokens and len(tokens[0]) <= 3:
                        position_text = tokens[0]
        else:
            # If no parent <tr>, try sibling elements
            sib = a.find_next_sibling()
            if sib:
                position_text = sib.get_text(strip=True)

        # Normalize and check for RB
        if position_text:
            pos_norm = position_text.upper()
        else:
            pos_norm = ''

        # Only include players whose position contains 'RB'
        if 'RB' in pos_norm:
            results.append({'team': team_abbrev, 'player_name': player_name, 'player_id': player_id})
            seen_ids.add(player_id)

    return results


def scrape_all_teams(team_abbrevs: List[str] = None, sleep_range=(MIN_SLEEP, MAX_SLEEP)) -> pd.DataFrame:
    """Scrape all teams and return DataFrame of RBs.

    - team_abbrevs: optional list to limit teams (useful for testing)
    - sleep_range: tuple(min,max) seconds to sleep between requests
    """
    team_abbrevs = team_abbrevs or TEAM_ABBREVS
    session = requests.Session()

    all_rows = []

    for team in team_abbrevs:
        try:
            team_rows = fetch_team_roster(team, session=session)
            logger.info('Found %d RBs for %s', len(team_rows), team)
            all_rows.extend(team_rows)
        except requests.exceptions.RequestException as e:
            logger.error('Network error for %s: %s', team, e)
        except Exception as e:
            logger.exception('Error parsing roster for %s: %s', team, e)

        # Rate limiting
        sleep_for = random.uniform(*sleep_range)
        logger.debug('Sleeping %.2fs', sleep_for)
        time.sleep(sleep_for)

    df = pd.DataFrame(all_rows, columns=['team', 'player_name', 'player_id'])
    return df


def ensure_output_dir():
    if not os.path.isdir(OUTPUT_DIR):
        os.makedirs(OUTPUT_DIR, exist_ok=True)


if __name__ == '__main__':
    # WARNING: This will make requests to ESPN for each team. Use responsibly.
    logger.info('Starting RB scraper for %d teams', len(TEAM_ABBREVS))

    # To avoid hitting ESPN too hard during development, you can pass a small
    # subset to `scrape_all_teams(['nyg','cin'])`.

    df = scrape_all_teams()

    if df.empty:
        logger.warning('No RBs found; exiting without writing CSV')
    else:
        ensure_output_dir()
        df.to_csv(OUTPUT_CSV, index=False)
        logger.info('Saved %d RB rows to %s', len(df), OUTPUT_CSV)
