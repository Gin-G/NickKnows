"""
Core NFL Data Loading Tasks
Handles base data imports from nflreadpy
"""
from nickknows import celery
import os
import nflreadpy as nfl
import pandas as pd
from celery.utils.log import get_task_logger
from datetime import datetime
from urllib.error import HTTPError

logger = get_task_logger(__name__)

SITE_DOMAIN = "https://www.nickknows.net"

def get_available_years():
    """Get available NFL years from 2020 to current available year"""
    return list(range(2020, 2026))

def get_selected_year():
    """Get selected year from context or default to latest available"""
    available_years = get_available_years()
    return max(available_years)

def format_nfl_season(year):
    """Format NFL season as 'YYYY-YYYY Season' (e.g., '2024-2025 Season')"""
    return f"{year-1}-{year} Season"

def get_data_path(year, data_type):
    """Get standardized data file path"""
    return os.getcwd() + f'/nickknows/nfl/data/{year}_{data_type}.csv'

@celery.task(name='nfl.core.update_pbp')
def update_pbp_data(year=None):
    """Update play-by-play data for specified year"""
    if year is None:
        year = get_selected_year()
    
    file_path = get_data_path(year, 'pbp_data')
    season_display = format_nfl_season(year)
    logger.info(f"Updating PBP data for {season_display}")
    
    try:
        # nflreadpy uses load_pbp() and returns polars DataFrame
        pbp_data = nfl.load_pbp(seasons=[year])
        
        # Convert polars to pandas if needed
        if hasattr(pbp_data, 'to_pandas'):
            pbp_data = pbp_data.to_pandas()
        
        pbp_data.to_csv(file_path, index=False)
        logger.info(f"✅ PBP data for {season_display} saved to {file_path}")
        return f"Successfully updated PBP data for {season_display}"
    except Exception as e:
        logger.error(f"❌ Error updating PBP data for {season_display}: {str(e)}")
        raise

@celery.task(name='nfl.core.update_rosters')
def update_roster_data(year=None):
    """Update roster data for specified year"""
    if year is None:
        year = get_selected_year()
        
    file_path = get_data_path(year, 'rosters')
    season_display = format_nfl_season(year)
    logger.info(f"Updating roster data for {season_display}")
    
    try:
        # nflreadpy uses load_rosters_weekly()
        roster_data = nfl.load_rosters_weekly(seasons=[year])
        if hasattr(roster_data, 'to_pandas'):
            roster_data = roster_data.to_pandas()
        roster_data.to_csv(file_path)
        logger.info(f"✅ Roster data for {season_display} saved to {file_path}")
        return f"Successfully updated roster data for {season_display}"
    except Exception as e:
        logger.error(f"❌ Error updating roster data for {season_display}: {str(e)}")
        raise


@celery.task(name='nfl.core.update_schedules')
def update_schedule_data(year=None):
    """Update schedule data for specified year"""
    if year is None:
        year = get_selected_year()
        
    file_path = get_data_path(year, 'schedule')
    season_display = format_nfl_season(year)
    logger.info(f"Updating schedule data for {season_display}")
    
    try:
        # nflreadpy uses load_schedules()
        schedule = nfl.load_schedules(seasons=[year])
        if hasattr(schedule, 'to_pandas'):
            schedule = schedule.to_pandas()
        schedule.to_csv(file_path)
        logger.info(f"✅ Schedule data for {season_display} saved to {file_path}")
        return f"Successfully updated schedule data for {season_display}"
    except Exception as e:
        logger.error(f"❌ Error updating schedule data for {season_display}: {str(e)}")
        raise


@celery.task(name='nfl.core.update_player_stats')
def update_player_stats_data(year=None):
    """Update player statistics for specified year"""
    if year is None:
        year = get_selected_year()
        
    file_path = get_data_path(year, 'weekly_data')
    season_display = format_nfl_season(year)
    logger.info(f"Updating player stats for {season_display}")
    
    try:
        # nflreadpy uses load_player_stats()
        # stat_type can be 'weekly' or 'season'
        player_stats = nfl.load_player_stats(
            seasons=[year],
            stat_type='weekly'
        )
        if hasattr(player_stats, 'to_pandas'):
            player_stats = player_stats.to_pandas()
        player_stats.to_csv(file_path)
        logger.info(f"✅ Player stats for {season_display} saved to {file_path}")
        logger.info(f"Created {len(player_stats)} player-week records")
        return f"Successfully updated player stats for {season_display}"
    except Exception as e:
        logger.error(f"❌ Error updating player stats for {season_display}: {str(e)}")
        raise


@celery.task(name='nfl.core.update_snap_counts')
def update_snap_counts_data(year=None):
    """Update snap count data for specified year"""
    if year is None:
        year = get_selected_year()
    
    file_path = get_data_path(year, 'snap_counts')
    season_display = format_nfl_season(year)
    logger.info(f"Updating snap counts for {season_display}")
    
    try:
        # nflreadpy uses load_snap_counts()
        snap_counts = nfl.load_snap_counts(seasons=[year])
        if hasattr(snap_counts, 'to_pandas'):
            snap_counts = snap_counts.to_pandas()
        snap_counts.to_csv(file_path)
        logger.info(f"✅ Snap counts for {season_display} saved to {file_path}")
        return f"Successfully updated snap counts for {season_display}"
    except Exception as e:
        logger.error(f"❌ Error updating snap counts for {season_display}: {str(e)}")
        raise


@celery.task(name='nfl.core.update_players')
def update_players_data():
    """Update player information (all players, not season-specific)"""
    file_path = os.getcwd() + '/nickknows/nfl/data/players.csv'
    logger.info("Updating player information database")
    
    try:
        # nflreadpy uses load_players()
        players = nfl.load_players()
        if hasattr(players, 'to_pandas'):
            players = players.to_pandas()
        players.to_csv(file_path)
        logger.info(f"✅ Player database saved to {file_path}")
        return "Successfully updated player database"
    except Exception as e:
        logger.error(f"❌ Error updating player database: {str(e)}")
        raise


@celery.task(name='nfl.core.update_all_base_data')
def update_all_base_data(year=None):
    """Update all base NFL data for a specific year"""
    if year is None:
        year = get_selected_year()
    
    season_display = format_nfl_season(year)
    logger.info(f"Starting full base data update for {season_display}")
    
    results = {
        'year': year,
        'season_display': season_display,
        'tasks': {}
    }
    
    # Update core data sequentially
    try:
        results['tasks']['pbp'] = update_pbp_data(year)
        results['tasks']['rosters'] = update_roster_data(year)
        results['tasks']['schedules'] = update_schedule_data(year)
        results['tasks']['player_stats'] = update_player_stats_data(year)
        results['tasks']['snap_counts'] = update_snap_counts_data(year)
        
        logger.info(f"✅ Completed all base data updates for {season_display}")
        return results
    except Exception as e:
        logger.error(f"❌ Error in base data update for {season_display}: {str(e)}")
        results['error'] = str(e)
        return results


@celery.task(name='nfl.core.check_data_availability')
def check_data_availability(year=None):
    """Check if data is available for a given year"""
    if year is None:
        year = get_selected_year()
    
    season_display = format_nfl_season(year)
    
    try:
        # Try to load schedule data as a lightweight check
        test_data = nfl.load_schedules(seasons=[year])
        
        if test_data.empty:
            return {
                'year': year,
                'season_display': season_display,
                'available': False,
                'message': f'No schedule data available for {season_display}'
            }
        else:
            return {
                'year': year,
                'season_display': season_display,
                'available': True,
                'message': f'Data available for {season_display} ({len(test_data)} games)',
                'games': len(test_data)
            }
    except Exception as e:
        return {
            'year': year,
            'season_display': season_display,
            'available': False,
            'message': f'Error checking {season_display}: {str(e)}'
        }