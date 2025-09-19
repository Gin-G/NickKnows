import pandas as pd
import numpy as np
import nfl_data_py as nfl
from collections import defaultdict

def validate_year_data(year):
    """Check if comprehensive snap data is available for a given year."""
    try:
        df = nfl.import_pbp_data([year])
        
        # Check for required participation columns
        required_columns = ['offense_players', 'defense_players']
        missing = [col for col in required_columns if col not in df.columns]
        
        if missing:
            return False, f"{year} missing participation columns: {missing}"
        
        # Check if columns have actual data
        participation_count = df['offense_players'].notna().sum()
        if participation_count == 0:
            return False, f"{year} has no participation data"
        
        return True, f"{year} has complete snap data ({participation_count} plays with participation)"
        
    except Exception as e:
        return False, f"Error loading {year}: {e}"

def load_nfl_data(year):
    """Load NFL data for the specified year with validation."""
    is_valid, message = validate_year_data(year)
    
    if not is_valid:
        print(f"Warning: {message}")
        if year > 2024:
            print(f"Falling back to 2024 data for accurate snap count analysis...")
            year = 2024
        else:
            raise ValueError(f"Cannot load valid data for {year}")
    
    print(f"Loading {year} NFL data...")
    df = nfl.import_pbp_data([year])
    print(f"Loaded {len(df)} plays from {year} season")
    return df, year

def extract_player_participation(row):
    """Extract comprehensive player participation from the participation columns."""
    participants = {
        'offensive_players': [],
        'defensive_players': [],
        'special_teams_players': []
    }
    
    # Get team info
    posteam = row.get('posteam')
    defteam = row.get('defteam')
    play_type = row.get('play_type', '')
    
    # Skip if no team info
    if pd.isna(posteam) or pd.isna(defteam):
        return participants
    
    # Extract offensive players
    offense_players = row.get('offense_players', '')
    offense_names = row.get('offense_names', '')
    offense_positions = row.get('offense_positions', '')
    
    if offense_players and str(offense_players).strip():
        player_ids = str(offense_players).split(';')
        player_names = str(offense_names).split(';') if offense_names else [''] * len(player_ids)
        player_positions = str(offense_positions).split(';') if offense_positions else [''] * len(player_ids)
        
        for i, player_id in enumerate(player_ids):
            if player_id.strip():
                player_info = {
                    'id': player_id.strip(),
                    'name': player_names[i].strip() if i < len(player_names) else '',
                    'position': player_positions[i].strip() if i < len(player_positions) else '',
                    'team': posteam
                }
                
                if play_type in ['kickoff', 'punt', 'field_goal', 'extra_point']:
                    participants['special_teams_players'].append(player_info)
                else:
                    participants['offensive_players'].append(player_info)
    
    # Extract defensive players
    defense_players = row.get('defense_players', '')
    defense_names = row.get('defense_names', '')
    defense_positions = row.get('defense_positions', '')
    
    if defense_players and str(defense_players).strip():
        player_ids = str(defense_players).split(';')
        player_names = str(defense_names).split(';') if defense_names else [''] * len(player_ids)
        player_positions = str(defense_positions).split(';') if defense_positions else [''] * len(player_ids)
        
        for i, player_id in enumerate(player_ids):
            if player_id.strip():
                player_info = {
                    'id': player_id.strip(),
                    'name': player_names[i].strip() if i < len(player_names) else '',
                    'position': player_positions[i].strip() if i < len(player_positions) else '',
                    'team': defteam
                }
                
                if play_type in ['kickoff', 'punt', 'field_goal', 'extra_point']:
                    participants['special_teams_players'].append(player_info)
                else:
                    participants['defensive_players'].append(player_info)
    
    return participants

def should_count_as_snap(row):
    """Determine if a play should count as a snap for stat purposes."""
    excluded_play_types = ['timeout', 'quarter_end', 'half_end', 'game_end', 
                          'two_minute_warning', '', 'game', 'end_game', 'end_quarter']
    
    play_type = row.get('play_type') or ''
    if play_type:
        play_type = play_type.lower()
    
    if play_type in excluded_play_types:
        return False
    
    if row.get('aborted_play', 0) == 1:
        return False
    
    # Only count if we have participation data
    offense_players = row.get('offense_players', '')
    defense_players = row.get('defense_players', '')
    
    if not offense_players and not defense_players:
        return False
    
    return True

def calculate_accurate_snap_counts(df):
    """Calculate accurate snap counts using comprehensive player participation data."""
    snap_counts = defaultdict(lambda: {
        'offensive_snaps': 0,
        'defensive_snaps': 0, 
        'special_teams_snaps': 0,
        'total_snaps': 0,
        'team': '',
        'name': '',
        'position': ''
    })
    
    total_offensive_snaps = 0
    total_defensive_snaps = 0
    total_special_teams_snaps = 0
    
    for idx, row in df.iterrows():
        if not should_count_as_snap(row):
            continue
            
        participants = extract_player_participation(row)
        play_type = row.get('play_type', '')
        
        # Count total snaps by type
        if play_type in ['kickoff', 'punt', 'field_goal', 'extra_point']:
            total_special_teams_snaps += 1
        else:
            total_offensive_snaps += 1
            total_defensive_snaps += 1
        
        # Update player snap counts
        for player in participants['offensive_players']:
            player_id = player['id']
            snap_counts[player_id]['offensive_snaps'] += 1
            snap_counts[player_id]['team'] = player['team']
            snap_counts[player_id]['name'] = player['name']
            snap_counts[player_id]['position'] = player['position']
            
        for player in participants['defensive_players']:
            player_id = player['id']
            snap_counts[player_id]['defensive_snaps'] += 1
            snap_counts[player_id]['team'] = player['team']
            snap_counts[player_id]['name'] = player['name']
            snap_counts[player_id]['position'] = player['position']
            
        for player in participants['special_teams_players']:
            player_id = player['id']
            snap_counts[player_id]['special_teams_snaps'] += 1
            snap_counts[player_id]['team'] = player['team']
            snap_counts[player_id]['name'] = player['name']
            snap_counts[player_id]['position'] = player['position']
    
    # Calculate total snaps for each player
    for player_id in snap_counts:
        snap_counts[player_id]['total_snaps'] = (
            snap_counts[player_id]['offensive_snaps'] + 
            snap_counts[player_id]['defensive_snaps'] + 
            snap_counts[player_id]['special_teams_snaps']
        )
    
    return dict(snap_counts), {
        'total_offensive_snaps': total_offensive_snaps,
        'total_defensive_snaps': total_defensive_snaps, 
        'total_special_teams_snaps': total_special_teams_snaps
    }

def get_team_weekly_snap_breakdown(team, year):
    """Get snap counts broken down by week for a specific team."""
    df, actual_year = load_nfl_data(year)
    team_df = df[(df['home_team'] == team) | (df['away_team'] == team)]
    weeks = sorted(team_df['week'].dropna().unique())
    
    weekly_data = {}
    season_totals = defaultdict(lambda: {
        'offensive_snaps': 0,
        'defensive_snaps': 0,
        'special_teams_snaps': 0,
        'name': '',
        'position': '',
        'team': team
    })
    
    for week in weeks:
        week_df = team_df[team_df['week'] == week]
        snap_counts, totals = calculate_accurate_snap_counts(week_df)
        
        team_players = {pid: data for pid, data in snap_counts.items() if data['team'] == team}
        
        weekly_data[week] = {
            'players': team_players,
            'totals': totals
        }
        
        # Add to season totals
        for player_id, data in team_players.items():
            season_totals[player_id]['offensive_snaps'] += data['offensive_snaps']
            season_totals[player_id]['defensive_snaps'] += data['defensive_snaps']
            season_totals[player_id]['special_teams_snaps'] += data['special_teams_snaps']
            season_totals[player_id]['name'] = data['name']
            season_totals[player_id]['position'] = data['position']
    
    return weekly_data, dict(season_totals), weeks, actual_year

def get_team_snap_analysis(team, year, position_filter=None):
    """
    Main function: Get complete snap count analysis for a team.
    
    Args:
        team: Team abbreviation (e.g., 'NE', 'KC', 'BUF')
        year: Season year (e.g., 2024)
        position_filter: List of positions to include (e.g., ['QB', 'RB', 'WR', 'TE'])
                        If None, includes all skill positions
    
    Returns:
        dict: Complete analysis with weekly breakdowns and season totals
    """
    print(f"Analyzing {team} snap counts for {year} season...")
    
    weekly_data, season_totals, weeks, actual_year = get_team_weekly_snap_breakdown(team, year)
    
    # Default to skill positions if no filter specified
    if position_filter is None:
        position_filter = ['QB', 'RB', 'WR', 'TE']
    
    # Filter players by position
    filtered_players = {pid: data for pid, data in season_totals.items() 
                       if data['position'] in position_filter}
    
    # Group by position and calculate percentages
    analysis_results = {
        'team': team,
        'year': actual_year,
        'requested_year': year,
        'weeks': weeks,
        'positions': {},
        'season_totals': season_totals,
        'weekly_data': weekly_data
    }
    
    # Group by position
    positions = defaultdict(list)
    for player_id, data in filtered_players.items():
        positions[data['position']].append((player_id, data))
    
    # Process each position
    for position, players in positions.items():
        players.sort(key=lambda x: x[1]['offensive_snaps'] + x[1]['defensive_snaps'], reverse=True)
        
        position_data = {
            'players': [],
            'weekly_breakdown': {}
        }
        
        for player_id, season_data in players:
            player_analysis = {
                'id': player_id,
                'name': season_data['name'],
                'total_snaps': season_data['offensive_snaps'] + season_data['defensive_snaps'],
                'offensive_snaps': season_data['offensive_snaps'],
                'defensive_snaps': season_data['defensive_snaps'],
                'weekly_snaps': {}
            }
            
            # Get weekly snap breakdown with percentages
            for week in weeks:
                if week in weekly_data and player_id in weekly_data[week]['players']:
                    week_data = weekly_data[week]['players'][player_id]
                    week_snaps = week_data['offensive_snaps'] + week_data['defensive_snaps']
                    
                    # Calculate percentage based on team's max snaps for this position this week
                    if position in ['QB', 'RB', 'WR', 'TE']:
                        team_off_players = [p for p in weekly_data[week]['players'].values() if p['offensive_snaps'] > 0]
                        if team_off_players:
                            max_off_snaps = max(p['offensive_snaps'] for p in team_off_players)
                            pct = (week_data['offensive_snaps'] / max_off_snaps * 100) if max_off_snaps > 0 else 0
                        else:
                            pct = 0
                    else:
                        team_def_players = [p for p in weekly_data[week]['players'].values() if p['defensive_snaps'] > 0]
                        if team_def_players:
                            max_def_snaps = max(p['defensive_snaps'] for p in team_def_players)
                            pct = (week_data['defensive_snaps'] / max_def_snaps * 100) if max_def_snaps > 0 else 0
                        else:
                            pct = 0
                    
                    player_analysis['weekly_snaps'][week] = {
                        'snaps': week_snaps,
                        'percentage': round(pct, 1)
                    }
                else:
                    player_analysis['weekly_snaps'][week] = {
                        'snaps': 0,
                        'percentage': 0
                    }
            
            position_data['players'].append(player_analysis)
        
        analysis_results['positions'][position] = position_data
    
    return analysis_results

def print_team_snap_analysis(team, year, position_filter=None):
    """Print formatted snap analysis for a team."""
    results = get_team_snap_analysis(team, year, position_filter)
    
    # Show year info if different from requested
    year_info = f"{results['year']}"
    if results['requested_year'] != results['year']:
        year_info += f" (requested {results['requested_year']})"
    
    print(f"\n{'='*80}")
    print(f"{team} - {year_info} WEEKLY SNAP COUNTS BREAKDOWN")
    print(f"{'='*80}")
    
    for position, position_data in results['positions'].items():
        print(f"\n{position}:")
        print("-" * 70)
        
        # Header
        week_headers = "".join([f"Wk {w:2}" for w in results['weeks']])
        print(f"{'Player':<25} {week_headers} {'Total':>8}")
        print("-" * 70)
        
        for player in position_data['players']:
            name = player['name']
            total_snaps = player['total_snaps']
            
            # Format weekly data
            weekly_snaps = []
            for week in results['weeks']:
                week_info = player['weekly_snaps'][week]
                if week_info['snaps'] > 0:
                    weekly_snaps.append(f"{week_info['snaps']:2}({week_info['percentage']:3.0f}%)")
                else:
                    weekly_snaps.append("   --   ")
            
            weekly_str = " ".join([f"{snap:>8}" for snap in weekly_snaps])
            print(f"{name:<25} {weekly_str} {total_snaps:>8}")
    
    # Print season summary
    print(f"\n{'='*60}")
    print(f"{team} - {year_info} SEASON SUMMARY")
    print(f"{'='*60}")
    
    for position, position_data in results['positions'].items():
        print(f"\n{position} Season Totals:")
        for player in position_data['players']:
            primary_snaps = player['offensive_snaps'] if position in ['QB', 'RB', 'WR', 'TE'] else player['defensive_snaps']
            print(f"  {player['name']:<25} {primary_snaps:>3} snaps")

def analyze_team_snap_counts(team, year, position_filter=None):
    """
    Main API function for getting team snap count analysis.
    Automatically handles data validation and fallback to valid years.
    
    Usage:
        # Get all skill positions
        results = analyze_team_snap_counts('KC', 2024)
        
        # Get specific positions
        results = analyze_team_snap_counts('BUF', 2024, ['QB', 'RB'])
        
        # Get defensive positions
        results = analyze_team_snap_counts('SF', 2024, ['CB', 'LB', 'DE'])
        
        # Try 2025 (will auto-fallback to 2024 with warning)
        results = analyze_team_snap_counts('NE', 2025)
    """
    return get_team_snap_analysis(team, year, position_filter)

def compare_teams(team1, team2, position='QB', year=2024):
    """Compare specific position between two teams."""
    print(f"Comparing {position} snap counts: {team1} vs {team2} ({year})")
    
    # Get data for both teams
    results1 = analyze_team_snap_counts(team1, year, [position])
    results2 = analyze_team_snap_counts(team2, year, [position])
    
    print(f"\n{team1} {position}s:")
    if position in results1['positions']:
        for player in results1['positions'][position]['players']:
            primary_snaps = player['offensive_snaps'] if position in ['QB', 'RB', 'WR', 'TE'] else player['defensive_snaps']
            print(f"  {player['name']:<25} {primary_snaps:>3} snaps")
    else:
        print(f"  No {position} data found")
    
    print(f"\n{team2} {position}s:")
    if position in results2['positions']:
        for player in results2['positions'][position]['players']:
            primary_snaps = player['offensive_snaps'] if position in ['QB', 'RB', 'WR', 'TE'] else player['defensive_snaps']
            print(f"  {player['name']:<25} {primary_snaps:>3} snaps")
    else:
        print(f"  No {position} data found")

def list_available_years():
    """Check which years have complete snap count data."""
    print("Checking data availability by year...")
    
    years_to_check = [2025, 2024, 2023, 2022, 2021, 2020]
    available_years = []
    
    for year in years_to_check:
        is_valid, message = validate_year_data(year)
        status = "✓" if is_valid else "✗"
        print(f"  {year}: {status} {message}")
        if is_valid:
            available_years.append(year)
    
    print(f"\nRecommended years for analysis: {available_years}")
    return available_years

if __name__ == "__main__":
    print("NFL Snap Count Analysis System")
    print("="*50)
    
    print("\nChecking data availability...")
    available_years = list_available_years()
    
    print("\nUsage Examples:")
    print("# Analyze any team for any year")
    print("results = analyze_team_snap_counts('KC', 2024)")
    print("results = analyze_team_snap_counts('BUF', 2023, ['QB', 'RB'])")
    print("results = analyze_team_snap_counts('SF', 2024, ['CB', 'LB', 'DE'])")
    
    print("\n# Print formatted analysis")
    print("print_team_snap_analysis('NE', 2024)")
    print("print_team_snap_analysis('KC', 2024, ['QB', 'WR'])")
    
    print("\n# Compare teams")
    print("compare_teams('NE', 'BUF', 'QB', 2024)")
    
    print("\n# Test with 2025 (will auto-fallback to 2024)")
    print("print_team_snap_analysis('KC', 2025)")
    
    # Example usage
    if available_years:
        latest_year = available_years[0]
        print(f"\nRunning example analysis with {latest_year} data...")
        print_team_snap_analysis('NE', latest_year)
    else:
        print("\nNo complete data available for analysis")