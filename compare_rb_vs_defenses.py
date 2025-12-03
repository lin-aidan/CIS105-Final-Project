"""
compare_rb_vs_defenses.py

Analyzes RB performance against top 16 (best) and bottom 16 (worst) run defenses.
Defense rankings are based on rushing yards allowed (lower = better defense).

Usage:
    python compare_rb_vs_defenses.py
"""

import logging
import pandas as pd

# Logging setup
logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s: %(message)s')
logger = logging.getLogger(__name__)

# File paths
DEFENSE_STATS = 'defense_stats.csv'
RB_GAMELOG = 'data/starting_rbs_gamelog.csv'
OUTPUT_VS_TOP = 'data/rb_performance_vs_top16_defenses.csv'
OUTPUT_VS_BOTTOM = 'data/rb_performance_vs_bottom16_defenses.csv'

# Map full team names to abbreviations used in game logs
TEAM_NAME_TO_ABBREV = {
    'New York Giants': 'nyg',
    'Cincinnati Bengals': 'cin',
    'Buffalo Bills': 'buf',
    'Miami Dolphins': 'mia',
    'Chicago Bears': 'chi',
    'Washington Commanders': 'wsh',
    'Atlanta Falcons': 'atl',
    'Tennessee Titans': 'ten',
    'Minnesota Vikings': 'min',
    'New York Jets': 'nyj',
    'Carolina Panthers': 'car',
    'Dallas Cowboys': 'dal',
    'New Orleans Saints': 'no',
    'Baltimore Ravens': 'bal',
    'Philadelphia Eagles': 'phi',
    'Arizona Cardinals': 'ari',
    'Los Angeles Chargers': 'lac',
    'San Francisco 49ers': 'sf',
    'Pittsburgh Steelers': 'pit',
    'Las Vegas Raiders': 'lv',
    'Los Angeles Rams': 'lar',
    'Cleveland Browns': 'cle',
    'Detroit Lions': 'det',
    'Kansas City Chiefs': 'kc',
    'Tampa Bay Buccaneers': 'tb',
    'Indianapolis Colts': 'ind',
    'Green Bay Packers': 'gb',
    'New England Patriots': 'ne',
    'Houston Texans': 'hou',
    'Seattle Seahawks': 'sea',
    'Denver Broncos': 'den',
    'Jacksonville Jaguars': 'jax',
}


def load_defense_rankings():
    """
    Load defense stats and rank by rushing yards allowed.
    Lower yards allowed = better defense.
    Returns top 16 (best defenses) and bottom 16 (worst defenses) as team abbreviations.
    """
    logger.info('Loading defense stats from %s', DEFENSE_STATS)
    df_def = pd.read_csv(DEFENSE_STATS)
    
    logger.info('Defense stats shape: %s', df_def.shape)
    logger.info('Columns: %s', df_def.columns.tolist())
    
    # Convert rushing yards to numeric (remove commas)
    if 'Rushing Yards (Yds)' in df_def.columns:
        df_def['Rushing Yards (Yds)'] = df_def['Rushing Yards (Yds)'].str.replace(',', '').astype(int)
    
    # Sort by rushing yards allowed (ascending = best defense)
    df_def = df_def.sort_values('Rushing Yards (Yds)')
    
    # Convert team names to abbreviations
    df_def['team_abbrev'] = df_def['Team'].map(TEAM_NAME_TO_ABBREV)
    
    # Top 16: best defenses (lowest rushing yards allowed)
    top16_defenses = df_def.head(16)['team_abbrev'].tolist()
    
    # Bottom 16: worst defenses (highest rushing yards allowed)
    bottom16_defenses = df_def.tail(16)['team_abbrev'].tolist()
    
    logger.info('Top 16 (best) defenses: %s', top16_defenses)
    logger.info('Bottom 16 (worst) defenses: %s', bottom16_defenses)
    
    return top16_defenses, bottom16_defenses


def load_rb_gamelogs():
    """Load RB game logs."""
    logger.info('Loading RB game logs from %s', RB_GAMELOG)
    df = pd.read_csv(RB_GAMELOG)
    return df


def normalize_opponent_abbrev(opp_string):
    """Extract team abbreviation from opponent string like '@SEA' or 'vsKC'."""
    if not opp_string or not isinstance(opp_string, str):
        return None
    
    # Remove @ or vs prefix
    opp = opp_string.replace('@', '').replace('vs', '').lower().strip()
    
    # Handle full names or extra text
    if len(opp) > 3:
        return None
    
    return opp


def analyze_rb_vs_defenses(df_games, top16_defenses, bottom16_defenses):
    """Analyze RB performance against top 16 and bottom 16 defenses."""
    logger.info('Analyzing RB performance...')
    
    results_top = []
    results_bottom = []
    
    # For each unique RB
    for player_name in df_games['player_name'].unique():
        player_games = df_games[df_games['player_name'] == player_name].copy()
        
        # Extract opponent abbreviations
        player_games['opponent_abbrev'] = player_games['opponent'].apply(normalize_opponent_abbrev)
        
        # Remove rows where opponent couldn't be parsed
        player_games = player_games[player_games['opponent_abbrev'].notna()]
        
        # Games vs top defenses
        games_vs_top = player_games[player_games['opponent_abbrev'].isin(top16_defenses)]
        
        # Games vs bottom defenses
        games_vs_bottom = player_games[player_games['opponent_abbrev'].isin(bottom16_defenses)]
        
        # Get team abbreviation
        team_abbrev = player_games['team'].iloc[0] if len(player_games) > 0 else None
        
        if len(games_vs_top) > 0:
            # Remove NaN values before summing
            rushing_yards_sum = games_vs_top['rushing_yards'].sum()
            rushing_attempts_sum = games_vs_top['rushing_attempts'].sum()
            rushing_td_sum = games_vs_top['rushing_td'].sum()
            receiving_yards_sum = games_vs_top['receiving_yards'].sum()
            receiving_recs_sum = games_vs_top['receiving_receptions'].sum()
            
            stats_top = {
                'player_name': player_name,
                'team': team_abbrev,
                'games_vs_top16': len(games_vs_top),
                'rushing_yards_vs_top16': rushing_yards_sum,
                'rushing_attempts_vs_top16': rushing_attempts_sum,
                'rushing_td_vs_top16': rushing_td_sum,
                'receiving_yards_vs_top16': receiving_yards_sum,
                'receiving_receptions_vs_top16': receiving_recs_sum,
            }
            # Calculate averages
            if stats_top['rushing_attempts_vs_top16'] > 0:
                stats_top['rushing_avg_vs_top16'] = round(stats_top['rushing_yards_vs_top16'] / stats_top['rushing_attempts_vs_top16'], 2)
            results_top.append(stats_top)
        
        if len(games_vs_bottom) > 0:
            # Remove NaN values before summing
            rushing_yards_sum = games_vs_bottom['rushing_yards'].sum()
            rushing_attempts_sum = games_vs_bottom['rushing_attempts'].sum()
            rushing_td_sum = games_vs_bottom['rushing_td'].sum()
            receiving_yards_sum = games_vs_bottom['receiving_yards'].sum()
            receiving_recs_sum = games_vs_bottom['receiving_receptions'].sum()
            
            stats_bottom = {
                'player_name': player_name,
                'team': team_abbrev,
                'games_vs_bottom16': len(games_vs_bottom),
                'rushing_yards_vs_bottom16': rushing_yards_sum,
                'rushing_attempts_vs_bottom16': rushing_attempts_sum,
                'rushing_td_vs_bottom16': rushing_td_sum,
                'receiving_yards_vs_bottom16': receiving_yards_sum,
                'receiving_receptions_vs_bottom16': receiving_recs_sum,
            }
            # Calculate averages
            if stats_bottom['rushing_attempts_vs_bottom16'] > 0:
                stats_bottom['rushing_avg_vs_bottom16'] = round(stats_bottom['rushing_yards_vs_bottom16'] / stats_bottom['rushing_attempts_vs_bottom16'], 2)
            results_bottom.append(stats_bottom)
    
    return pd.DataFrame(results_top), pd.DataFrame(results_bottom)


def main():
    """Main entry point."""
    logger.info('Starting RB vs defense analysis')
    
    # Load data
    top16_defenses, bottom16_defenses = load_defense_rankings()
    df_games = load_rb_gamelogs()
    
    # Analyze
    df_top, df_bottom = analyze_rb_vs_defenses(df_games, top16_defenses, bottom16_defenses)
    
    # Save results
    if len(df_top) > 0:
        df_top = df_top.sort_values('rushing_yards_vs_top16', ascending=False)
        df_top.to_csv(OUTPUT_VS_TOP, index=False)
        logger.info('Saved %d RBs performance vs top 16 defenses to %s', len(df_top), OUTPUT_VS_TOP)
        logger.info('Top RBs vs best defenses:\n%s', df_top.head(10))
    
    if len(df_bottom) > 0:
        df_bottom = df_bottom.sort_values('rushing_yards_vs_bottom16', ascending=False)
        df_bottom.to_csv(OUTPUT_VS_BOTTOM, index=False)
        logger.info('Saved %d RBs performance vs bottom 16 defenses to %s', len(df_bottom), OUTPUT_VS_BOTTOM)
        logger.info('Top RBs vs worst defenses:\n%s', df_bottom.head(10))


if __name__ == '__main__':
    main()
