import requests
from bs4 import BeautifulSoup
import pandas as pd
import json
import re

# URL to scrape
URL = "https://www.espn.com/nfl/stats/team/_/view/defense/table/rushing/sort/rushingYards/dir/desc"

# User agent to mimic a real browser
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
}

def scrape_espn_defense_stats():
    """
    Scrapes NFL defense rushing statistics from ESPN.
    Returns a DataFrame with team name, rushing yards (yds), and yards per game (ypg).
    """
    try:
        # Make request with user agent
        response = requests.get(URL, headers=HEADERS, timeout=10)
        response.raise_for_status()
        
        # Parse HTML
        soup = BeautifulSoup(response.content, 'html.parser')
        
        # Find the JSON data in the script tag
        scripts = soup.find_all('script')
        team_stats = None
        
        for script in scripts:
            if script.string and 'teamStats' in script.string:
                # Extract teamStats array using regex
                match = re.search(r'"teamStats":\[(.*?)\],"dictionary"', script.string, re.DOTALL)
                if match:
                    team_stats_str = '[' + match.group(1) + ']'
                    try:
                        team_stats = json.loads(team_stats_str)
                        break
                    except:
                        pass
        
        if team_stats is None:
            print("Could not find stats data on the page")
            return None
        
        # Extract team stats from the JSON
        data = []
        for team_data in team_stats:
            team_name = team_data['team']['displayName']
            
            # Find rushing yards and yards per game in stats
            rushing_yds = None
            rushing_ypg = None
            
            for stat in team_data['stats']:
                if stat['name'] == 'rushingYards':
                    rushing_yds = stat['value']
                elif stat['name'] == 'rushingYardsPerGame':
                    rushing_ypg = stat['value']
            
            data.append({
                'Team': team_name,
                'Rushing Yards (Yds)': rushing_yds if rushing_yds else "N/A",
                'Yards Per Game (Y/G)': rushing_ypg if rushing_ypg else "N/A"
            })
        
        # Create DataFrame
        df = pd.DataFrame(data)
        
        return df
    
    except requests.exceptions.RequestException as e:
        print(f"Error fetching the page: {e}")
        return None
    except Exception as e:
        print(f"Error parsing the data: {e}")
        return None

if __name__ == "__main__":
    # Scrape the data
    df = scrape_espn_defense_stats()
    
    if df is not None:
        print(f"Successfully scraped {len(df)} teams")
        print("\nFirst 10 rows:")
        print(df.head(10))
        print("\nColumn names:")
        print(df.columns.tolist())
        
        # Save to CSV
        df.to_csv('defense_stats.csv', index=False)
        print("\nData saved to defense_stats.csv")
    else:
        print("Failed to scrape the data")
