import pandas as pd
import numpy as np
import nfl_data_py as nfl
from collections import defaultdict

def get_team_snap_counts(team, year, position_filter=None):
    """
    Get snap count data for a specific team and year.
    
    Args:
        team: Team abbreviation (e.g., 'NE', 'KC', 'BUF')
        year: Season year (e.g., 2024, 2025)
        position_filter: List of positions to include (e.g., ['QB', 'RB', 'WR', 'TE'])
                        If None, includes all positions
    
    Returns:
        dict: Structured snap count data with weekly breakdowns and season totals
    """
    print(f"Loading {team} snap counts for {year}...")
    
    # Load snap count data
    df = nfl.import_snap_counts([year])
    
    # Filter to specific team
    team_df = df[df['team'] == team].copy()
    
    if len(team_df) == 0:
        return {
            'team': team,
            'year': year,
            'error': f'No data found for {team} in {year}',
            'weeks': [],
            'players': {},
            'totals': {}
        }
    
    # Apply position filter if specified
    if position_filter:
        team_df = team_df[team_df['position'].isin(position_filter)]
    
    # Get available weeks
    weeks = sorted(team_df['week'].unique())
    
    # Structure data by player
    players = {}
    
    for _, row in team_df.iterrows():
        player_name = row['player']
        
        if player_name not in players:
            players[player_name] = {
                'name': player_name,
                'position': row['position'],
                'pfr_id': row['pfr_player_id'],
                'weekly_snaps': {},
                'season_totals': {
                    'offense_snaps': 0,
                    'defense_snaps': 0,
                    'st_snaps': 0,
                    'total_snaps': 0
                }
            }
        
        week = row['week']
        
        # Weekly data
        players[player_name]['weekly_snaps'][week] = {
            'offense_snaps': int(row['offense_snaps']) if pd.notna(row['offense_snaps']) else 0,
            'offense_pct': round(row['offense_pct'] * 100, 1) if pd.notna(row['offense_pct']) else 0.0,
            'defense_snaps': int(row['defense_snaps']) if pd.notna(row['defense_snaps']) else 0,
            'defense_pct': round(row['defense_pct'] * 100, 1) if pd.notna(row['defense_pct']) else 0.0,
            'st_snaps': int(row['st_snaps']) if pd.notna(row['st_snaps']) else 0,
            'st_pct': round(row['st_pct'] * 100, 1) if pd.notna(row['st_pct']) else 0.0,
            'opponent': row['opponent']
        }
        
        # Add to season totals
        players[player_name]['season_totals']['offense_snaps'] += int(row['offense_snaps']) if pd.notna(row['offense_snaps']) else 0
        players[player_name]['season_totals']['defense_snaps'] += int(row['defense_snaps']) if pd.notna(row['defense_snaps']) else 0
        players[player_name]['season_totals']['st_snaps'] += int(row['st_snaps']) if pd.notna(row['st_snaps']) else 0
    
    # Calculate total snaps for each player
    for player in players.values():
        player['season_totals']['total_snaps'] = (
            player['season_totals']['offense_snaps'] + 
            player['season_totals']['defense_snaps'] + 
            player['season_totals']['st_snaps']
        )
    
    return {
        'team': team,
        'year': year,
        'weeks': weeks,
        'players': players,
        'data_summary': {
            'total_players': len(players),
            'weeks_available': len(weeks),
            'positions': sorted(list(set(p['position'] for p in players.values())))
        }
    }

def get_team_snap_summary(team, year, position_filter=None):
    """
    Get a summary view of team snap counts organized by position.
    
    Returns:
        dict: Snap counts organized by position groups
    """
    data = get_team_snap_counts(team, year, position_filter)
    
    if 'error' in data:
        return data
    
    # Group players by position
    positions = defaultdict(list)
    
    for player_name, player_data in data['players'].items():
        position = player_data['position']
        positions[position].append({
            'name': player_name,
            'offense_snaps': player_data['season_totals']['offense_snaps'],
            'defense_snaps': player_data['season_totals']['defense_snaps'],
            'st_snaps': player_data['season_totals']['st_snaps'],
            'total_snaps': player_data['season_totals']['total_snaps']
        })
    
    # Sort players within each position by total snaps
    for position in positions:
        positions[position].sort(key=lambda x: x['total_snaps'], reverse=True)
    
    return {
        'team': data['team'],
        'year': data['year'],
        'weeks': data['weeks'],
        'positions': dict(positions),
        'data_summary': data['data_summary']
    }

def get_weekly_snap_breakdown(team, year, week, position_filter=None):
    """
    Get detailed snap counts for a specific team, year, and week.
    
    Returns:
        dict: Player snap counts for the specified week
    """
    data = get_team_snap_counts(team, year, position_filter)
    
    if 'error' in data:
        return data
    
    if week not in data['weeks']:
        return {
            'team': team,
            'year': year,
            'week': week,
            'error': f'Week {week} not available. Available weeks: {data["weeks"]}',
            'players': {}
        }
    
    # Extract week-specific data
    week_players = {}
    
    for player_name, player_data in data['players'].items():
        if week in player_data['weekly_snaps']:
            week_data = player_data['weekly_snaps'][week]
            week_players[player_name] = {
                'name': player_name,
                'position': player_data['position'],
                'opponent': week_data['opponent'],
                'offense_snaps': week_data['offense_snaps'],
                'offense_pct': week_data['offense_pct'],
                'defense_snaps': week_data['defense_snaps'],
                'defense_pct': week_data['defense_pct'],
                'st_snaps': week_data['st_snaps'],
                'st_pct': week_data['st_pct'],
                'total_snaps': week_data['offense_snaps'] + week_data['defense_snaps'] + week_data['st_snaps']
            }
    
    return {
        'team': team,
        'year': year,
        'week': week,
        'players': week_players,
        'data_summary': {
            'total_players': len(week_players),
            'positions': sorted(list(set(p['position'] for p in week_players.values())))
        }
    }

def get_player_snap_history(player_name, team, year):
    """
    Get snap count history for a specific player.
    
    Returns:
        dict: Week-by-week snap counts for the player
    """
    data = get_team_snap_counts(team, year)
    
    if 'error' in data:
        return data
    
    if player_name not in data['players']:
        available_players = list(data['players'].keys())
        return {
            'team': team,
            'year': year,
            'player': player_name,
            'error': f'Player {player_name} not found. Available players: {available_players[:10]}...',
            'weekly_snaps': {}
        }
    
    player_data = data['players'][player_name]
    
    return {
        'team': team,
        'year': year,
        'player': player_name,
        'position': player_data['position'],
        'pfr_id': player_data['pfr_id'],
        'weekly_snaps': player_data['weekly_snaps'],
        'season_totals': player_data['season_totals'],
        'weeks_available': data['weeks']
    }

def compare_player_snap_counts(players_list, year):
    """
    Compare snap counts between multiple players.
    
    Args:
        players_list: List of tuples [(player_name, team), ...]
        year: Season year
    
    Returns:
        dict: Comparison data for all players
    """
    comparison = {
        'year': year,
        'players': {},
        'summary': {
            'total_players': len(players_list),
            'weeks_compared': set()
        }
    }
    
    for player_name, team in players_list:
        player_data = get_player_snap_history(player_name, team, year)
        
        if 'error' not in player_data:
            comparison['players'][f"{player_name} ({team})"] = player_data
            comparison['summary']['weeks_compared'].update(player_data['weeks_available'])
    
    comparison['summary']['weeks_compared'] = sorted(list(comparison['summary']['weeks_compared']))
    
    return comparison

def get_available_data_years():
    """
    Check which years have snap count data available.
    
    Returns:
        dict: Available years and their data summary
    """
    available_years = {}
    years_to_check = [2025, 2024, 2023, 2022, 2021, 2020]
    
    for year in years_to_check:
        try:
            df = nfl.import_snap_counts([year])
            
            if len(df) > 0:
                weeks = sorted(df['week'].unique())
                teams = sorted(df['team'].unique())
                
                available_years[year] = {
                    'total_records': len(df),
                    'teams': len(teams),
                    'weeks': weeks,
                    'week_range': f"{weeks[0]}-{weeks[-1]}" if weeks else "No weeks",
                    'positions': sorted(df['position'].unique().tolist())
                }
            
        except Exception as e:
            available_years[year] = {
                'error': str(e),
                'available': False
            }
    
    return available_years

def format_snap_counts_for_display(team, year, position_filter=None):
    """
    Format snap count data for web display (Flask-ready).
    
    Returns:
        dict: Clean, formatted data ready for JSON serialization
    """
    data = get_team_snap_summary(team, year, position_filter)
    
    if 'error' in data:
        return {
            'success': False,
            'error': data['error'],
            'team': team,
            'year': year
        }
    
    # Format for display
    formatted_positions = {}
    
    for position, players in data['positions'].items():
        formatted_positions[position] = []
        
        for player in players:
            formatted_positions[position].append({
                'name': player['name'],
                'offense_snaps': player['offense_snaps'],
                'defense_snaps': player['defense_snaps'],
                'st_snaps': player['st_snaps'],
                'total_snaps': player['total_snaps']
            })
    
    return {
        'success': True,
        'team': data['team'],
        'year': data['year'],
        'weeks': data['weeks'],
        'positions': formatted_positions,
        'summary': {
            'total_players': data['data_summary']['total_players'],
            'weeks_available': data['data_summary']['weeks_available'],
            'position_groups': len(formatted_positions),
            'positions_list': data['data_summary']['positions']
        }
    }

# Utility functions for Flask integration
def get_all_teams():
    """Get list of all NFL teams."""
    return ['ARI', 'ATL', 'BAL', 'BUF', 'CAR', 'CHI', 'CIN', 'CLE', 'DAL', 'DEN', 
            'DET', 'GB', 'HOU', 'IND', 'JAX', 'KC', 'LA', 'LAC', 'LV', 'MIA', 
            'MIN', 'NE', 'NO', 'NYG', 'NYJ', 'PHI', 'PIT', 'SEA', 'SF', 'TB', 'TEN', 'WAS']

def get_skill_positions():
    """Get list of skill position abbreviations."""
    return ['QB', 'RB', 'WR', 'TE']

def get_defensive_positions():
    """Get list of defensive position abbreviations."""
    return ['CB', 'DB', 'DE', 'DT', 'LB', 'FS', 'SS', 'S', 'NT']

def get_offensive_line_positions():
    """Get list of offensive line position abbreviations."""
    return ['C', 'G', 'T', 'OL']

if __name__ == "__main__":
    # Example usage
    print("NFL Snap Counts API Functions")
    print("="*40)
    
    # Check available years
    print("\nChecking available years...")
    years = get_available_data_years()
    for year, info in years.items():
        if 'error' not in info:
            print(f"  {year}: {info['total_records']} records, weeks {info['week_range']}")
        else:
            print(f"  {year}: Error - {info['error']}")
    
    # Example: Get Patriots 2025 data
    print(f"\nExample: Patriots 2025 snap counts...")
    ne_data = format_snap_counts_for_display('NE', 2025, ['QB', 'RB', 'WR', 'TE'])
    
    if ne_data['success']:
        print(f"Success! Found {ne_data['summary']['total_players']} players")
        print(f"Weeks: {ne_data['weeks']}")
        print(f"Positions: {list(ne_data['positions'].keys())}")
    else:
        print(f"Error: {ne_data['error']}")
    
    print(f"\nReady for Flask integration!")
    print(f"Use format_snap_counts_for_display() for JSON responses")