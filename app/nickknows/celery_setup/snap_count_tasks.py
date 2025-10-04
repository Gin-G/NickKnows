"""
NFL Snap Count Tasks
Handles snap count data loading and processing
"""
from nickknows import celery
import os
import pandas as pd
from collections import defaultdict
from celery.utils.log import get_task_logger
from datetime import datetime

logger = get_task_logger(__name__)


def get_data_path(year, data_type):
    """Get standardized data file path"""
    return os.getcwd() + f'/nickknows/nfl/data/{year}_{data_type}.csv'


def get_team_data_path(team, year, data_type):
    """Get team-specific data file path"""
    return os.getcwd() + f'/nickknows/nfl/data/{team}/{year}_{team}_{data_type}.csv'


def format_nfl_season(year):
    """Format NFL season display name"""
    return f"{year-1}-{year} Season"


@celery.task(name='nfl.snaps.update_team_snap_counts')
def update_team_snap_counts(team, year):
    """Update snap count data for a specific team"""
    season_display = format_nfl_season(year)
    logger.info(f"Updating snap counts for {team} ({season_display})")
    
    try:
        # Ensure team directory exists
        team_dir = os.getcwd() + f'/nickknows/nfl/data/{team}/'
        os.makedirs(team_dir, exist_ok=True)
        
        # Check if global snap count data exists
        snap_path = get_data_path(year, 'snap_counts')
        
        if not os.path.exists(snap_path):
            # Load fresh data
            import nflreadpy as nfl
            logger.info(f"Loading snap count data for {year}")
            snap_data = nfl.load_snap_counts(seasons=[year])
        else:
            # Use cached data
            snap_data = pd.read_csv(snap_path, index_col=0)
        
        if snap_data.empty:
            logger.warning(f"No snap count data available for {year}")
            return f"No snap count data for {year}"
        
        # Filter to team
        team_snaps = snap_data[snap_data['team'] == team].copy()
        
        if team_snaps.empty:
            logger.warning(f"No snap data for {team} in {year}")
            return f"No snap data for {team} in {year}"
        
        # Clean and process
        team_snaps = team_snaps.fillna(0)
        
        # Ensure numeric columns
        numeric_cols = ['offense_snaps', 'defense_snaps', 'st_snaps', 
                       'offense_pct', 'defense_pct', 'st_pct']
        for col in numeric_cols:
            if col in team_snaps.columns:
                team_snaps[col] = pd.to_numeric(team_snaps[col], errors='coerce').fillna(0)
        
        # Calculate total snaps
        team_snaps['total_snaps'] = (
            team_snaps['offense_snaps'] +
            team_snaps['defense_snaps'] +
            team_snaps['st_snaps']
        )
        
        # Sort by week and player
        team_snaps = team_snaps.sort_values(['week', 'player'])
        
        # Save
        output_path = get_team_data_path(team, year, 'snap_counts')
        team_snaps.to_csv(output_path, index=False)
        
        logger.info(f"✅ Snap counts saved for {team} ({season_display}): {len(team_snaps)} records")
        return f"Updated snap counts for {team} ({season_display})"
        
    except Exception as e:
        logger.error(f"❌ Error updating snap counts for {team} ({season_display}): {str(e)}")
        raise


@celery.task(name='nfl.snaps.update_all_teams')
def update_all_teams_snap_counts(year):
    """Update snap counts for all NFL teams"""
    season_display = format_nfl_season(year)
    logger.info(f"Starting snap count updates for all teams ({season_display})")
    
    teams = [
        'ARI', 'ATL', 'BAL', 'BUF', 'CAR', 'CHI', 'CIN', 'CLE',
        'DAL', 'DEN', 'DET', 'GB', 'HOU', 'IND', 'JAX', 'KC',
        'LA', 'LAC', 'LV', 'MIA', 'MIN', 'NE', 'NO', 'NYG',
        'NYJ', 'PHI', 'PIT', 'SEA', 'SF', 'TB', 'TEN', 'WAS'
    ]
    
    # Schedule updates for all teams
    for team in teams:
        update_team_snap_counts.delay(team, year)
    
    logger.info(f"Scheduled snap count updates for {len(teams)} teams ({season_display})")
    return f"Scheduled snap count updates for all teams ({season_display})"


@celery.task(name='nfl.snaps.get_team_summary')
def get_team_snap_summary(team, year, position_filter=None):
    """Get formatted snap count summary for a team"""
    logger.info(f"Getting snap summary for {team} - {year}")
    
    try:
        snap_path = get_team_data_path(team, year, 'snap_counts')
        
        if not os.path.exists(snap_path):
            update_team_snap_counts.delay(team, year)
            return {
                'success': False,
                'error': f'Snap data not available for {team} ({year}). Update in progress.',
                'team': team,
                'year': year
            }
        
        # Load data
        df = pd.read_csv(snap_path)
        
        if df.empty:
            return {
                'success': False,
                'error': f'No snap data for {team} ({year})',
                'team': team,
                'year': year
            }
        
        # Apply position filter
        if position_filter:
            df = df[df['position'].isin(position_filter)]
        
        # Group by position
        positions = defaultdict(list)
        weeks = sorted(df['week'].unique()) if 'week' in df.columns else []
        
        # Calculate season totals per player
        player_totals = df.groupby(['player', 'position']).agg({
            'offense_snaps': 'sum',
            'defense_snaps': 'sum',
            'st_snaps': 'sum',
            'total_snaps': 'sum'
        }).reset_index()
        
        for _, row in player_totals.iterrows():
            position = row['position']
            positions[position].append({
                'name': row['player'],
                'offense_snaps': int(row['offense_snaps']),
                'defense_snaps': int(row['defense_snaps']),
                'st_snaps': int(row['st_snaps']),
                'total_snaps': int(row['total_snaps'])
            })
        
        # Sort by total snaps
        for position in positions:
            positions[position].sort(key=lambda x: x['total_snaps'], reverse=True)
        
        return {
            'success': True,
            'team': team,
            'year': year,
            'weeks': weeks,
            'positions': dict(positions),
            'summary': {
                'total_players': len(player_totals),
                'weeks_available': len(weeks),
                'position_groups': len(positions),
                'positions_list': sorted(positions.keys())
            }
        }
        
    except Exception as e:
        logger.error(f"Error getting snap summary for {team} ({year}): {str(e)}")
        return {
            'success': False,
            'error': str(e),
            'team': team,
            'year': year
        }


@celery.task(name='nfl.snaps.get_weekly_breakdown')
def get_weekly_snap_breakdown(team, year, week):
    """Get snap counts for a specific week"""
    logger.info(f"Getting weekly snaps for {team} - {year} Week {week}")
    
    try:
        snap_path = get_team_data_path(team, year, 'snap_counts')
        
        if not os.path.exists(snap_path):
            return {
                'success': False,
                'error': f'Snap data not available for {team} ({year})',
                'team': team,
                'year': year,
                'week': week
            }
        
        df = pd.read_csv(snap_path)
        week_df = df[df['week'] == week]
        
        if week_df.empty:
            available_weeks = sorted(df['week'].unique()) if 'week' in df.columns else []
            return {
                'success': False,
                'error': f'Week {week} not available. Available: {available_weeks}',
                'team': team,
                'year': year,
                'week': week
            }
        
        # Format player data
        players = {}
        for _, row in week_df.iterrows():
            player_name = row['player']
            players[player_name] = {
                'name': player_name,
                'position': row['position'],
                'opponent': row.get('opponent', 'Unknown'),
                'offense_snaps': int(row['offense_snaps']) if pd.notna(row['offense_snaps']) else 0,
                'offense_pct': round(row['offense_pct'] * 100, 1) if pd.notna(row['offense_pct']) else 0.0,
                'defense_snaps': int(row['defense_snaps']) if pd.notna(row['defense_snaps']) else 0,
                'defense_pct': round(row['defense_pct'] * 100, 1) if pd.notna(row['defense_pct']) else 0.0,
                'st_snaps': int(row['st_snaps']) if pd.notna(row['st_snaps']) else 0,
                'st_pct': round(row['st_pct'] * 100, 1) if pd.notna(row['st_pct']) else 0.0,
                'total_snaps': int(row['total_snaps']) if pd.notna(row['total_snaps']) else 0
            }
        
        return {
            'success': True,
            'team': team,
            'year': year,
            'week': week,
            'players': players,
            'summary': {
                'total_players': len(players),
                'positions': sorted(list(set(p['position'] for p in players.values())))
            }
        }
        
    except Exception as e:
        logger.error(f"Error getting weekly snaps for {team} ({year}) Week {week}: {str(e)}")
        return {
            'success': False,
            'error': str(e),
            'team': team,
            'year': year,
            'week': week
        }


@celery.task(name='nfl.snaps.check_availability')
def check_snap_count_availability(year=None):
    """Check which years have snap count data available"""
    years_to_check = [year] if year else [2025, 2024, 2023, 2022, 2021, 2020]
    
    available_years = {}
    
    for check_year in years_to_check:
        try:
            import nflreadpy as nfl
            logger.info(f"Checking snap count availability for {check_year}")
            df = nfl.load_snap_counts(seasons=[check_year])
            
            if len(df) > 0:
                weeks = sorted(df['week'].unique())
                teams = sorted(df['team'].unique())
                
                available_years[check_year] = {
                    'available': True,
                    'total_records': len(df),
                    'teams': len(teams),
                    'team_list': teams,
                    'weeks': weeks,
                    'week_range': f"{weeks[0]}-{weeks[-1]}" if weeks else "No weeks",
                    'positions': sorted(df['position'].unique().tolist())
                }
                
                logger.info(f"Year {check_year}: {len(df)} records, {len(teams)} teams")
            else:
                available_years[check_year] = {
                    'available': False,
                    'error': 'No data returned'
                }
            
        except Exception as e:
            logger.error(f"Error checking {check_year}: {str(e)}")
            available_years[check_year] = {
                'available': False,
                'error': str(e)
            }
    
    return available_years