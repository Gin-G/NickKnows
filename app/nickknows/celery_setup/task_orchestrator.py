"""
NFL Data Task Orchestrator
Main entry points for coordinating multi-step data updates
"""
from nickknows import celery
from celery import chain, chord, group
from celery.utils.log import get_task_logger
import time

logger = get_task_logger(__name__)


def format_nfl_season(year):
    """Format NFL season display name"""
    return f"{year-1}-{year} Season"


@celery.task(name='nfl.orchestrator.update_full_season')
def update_full_season_data(year):
    """
    Complete season data update workflow
    1. Core data (PBP, rosters, schedules, player stats, snap counts)
    2. Statistical aggregations (top 10 leaders)
    3. Team analysis (FPA for all teams)
    4. Opportunity tracking
    """
    season_display = format_nfl_season(year)
    logger.info(f"Starting full season update for {season_display}")
    
    # Import task modules
    from .core_data_tasks import (
        update_pbp_data,
        update_roster_data,
        update_schedule_data,
        update_player_stats_data,
        update_snap_counts_data
    )
    from .stat_aggregation_tasks import calculate_all_stat_leaders
    from .team_analysis_tasks import update_all_team_fpa
    from .opportunity_tasks import calculate_opportunity_data
    
    # Create workflow
    workflow = chain(
        # Step 1: Core data (parallel)
        group(
            update_pbp_data.si(year),
            update_roster_data.si(year),
            update_schedule_data.si(year),
            update_player_stats_data.si(year),
            update_snap_counts_data.si(year)
        ),
        
        # Step 2: Statistical aggregations (depends on player stats)
        calculate_all_stat_leaders.si(year),
        
        # Step 3: Team analysis (depends on schedules and player stats)
        update_all_team_fpa.si(year),
        
        # Step 4: Opportunity tracking (depends on PBP and rosters)
        calculate_opportunity_data.si(year)
    )
    
    # Execute workflow
    result = workflow.apply_async()
    
    logger.info(f"Full season update workflow started for {season_display}")
    return {
        'year': year,
        'season_display': season_display,
        'task_id': result.id,
        'message': f"Full update workflow initiated for {season_display}"
    }


@celery.task(name='nfl.orchestrator.update_core_only')
def update_core_data_only(year):
    """
    Update only core data without derived calculations
    Faster update for when you just need fresh base data
    """
    season_display = format_nfl_season(year)
    logger.info(f"Starting core data update for {season_display}")
    
    from .core_data_tasks import update_all_base_data
    
    result = update_all_base_data.delay(year)
    
    return {
        'year': year,
        'season_display': season_display,
        'task_id': result.id,
        'message': f"Core data update initiated for {season_display}"
    }


@celery.task(name='nfl.orchestrator.update_single_team_full')
def update_single_team_full(team, year):
    """
    Complete update for a single team
    1. Team schedule
    2. Weekly team data
    3. FPA calculations
    4. Snap counts
    5. Opportunities
    """
    season_display = format_nfl_season(year)
    logger.info(f"Starting full update for {team} ({season_display})")
    
    from .team_analysis_tasks import update_single_team_data
    from .snap_count_tasks import update_team_snap_counts
    from .opportunity_tasks import update_team_opportunity_data
    
    # Create workflow
    workflow = chain(
        update_single_team_data.si(team, year),
        update_team_snap_counts.si(team, year),
        update_team_opportunity_data.si(team, year)
    )
    
    result = workflow.apply_async()
    
    return {
        'team': team,
        'year': year,
        'season_display': season_display,
        'task_id': result.id,
        'message': f"Full team update initiated for {team} ({season_display})"
    }


@celery.task(name='nfl.orchestrator.update_current_week')
def update_current_week_data(year, week):
    """
    Quick update for current week's data
    Useful for in-season updates
    """
    season_display = format_nfl_season(year)
    logger.info(f"Updating Week {week} data for {season_display}")
    
    from .core_data_tasks import (
        update_schedule_data,
        update_player_stats_data
    )
    from .stat_aggregation_tasks import calculate_all_stat_leaders
    
    # Quick workflow for current week
    workflow = chain(
        group(
            update_schedule_data.si(year),
            update_player_stats_data.si(year)
        ),
        calculate_all_stat_leaders.si(year)
    )
    
    result = workflow.apply_async()
    
    return {
        'year': year,
        'week': week,
        'season_display': season_display,
        'task_id': result.id,
        'message': f"Week {week} update initiated for {season_display}"
    }


@celery.task(name='nfl.orchestrator.update_opportunities_only')
def update_opportunities_only(year):
    """
    Update only opportunity tracking data
    Faster than full update when you just need opportunity metrics
    """
    season_display = format_nfl_season(year)
    logger.info(f"Updating opportunity data for {season_display}")
    
    from .opportunity_tasks import calculate_opportunity_data
    
    result = calculate_opportunity_data.delay(year)
    
    return {
        'year': year,
        'season_display': season_display,
        'task_id': result.id,
        'message': f"Opportunity update initiated for {season_display}"
    }


@celery.task(name='nfl.orchestrator.update_snap_counts_only')
def update_snap_counts_only(year):
    """
    Update only snap count data for all teams
    """
    season_display = format_nfl_season(year)
    logger.info(f"Updating snap counts for {season_display}")
    
    from .snap_count_tasks import update_all_teams_snap_counts
    
    result = update_all_teams_snap_counts.delay(year)
    
    return {
        'year': year,
        'season_display': season_display,
        'task_id': result.id,
        'message': f"Snap count update initiated for {season_display}"
    }


@celery.task(name='nfl.orchestrator.health_check')
def system_health_check(year):
    """
    Check data availability and file status for a season
    """
    import os
    season_display = format_nfl_season(year)
    logger.info(f"Running health check for {season_display}")
    
    data_dir = os.getcwd() + '/nickknows/nfl/data/'
    
    # Check core data files
    core_files = {
        'pbp': f'{year}_pbp_data.csv',
        'rosters': f'{year}_rosters.csv',
        'schedules': f'{year}_schedule.csv',
        'player_stats': f'{year}_weekly_data.csv',
        'snap_counts': f'{year}_snap_counts.csv'
    }
    
    # Check stat files
    stat_files = {
        'qb_yards': f'{year}_qb_yards_top10_data.csv',
        'qb_tds': f'{year}_qb_tds_top10_data.csv',
        'rb_yards': f'{year}_rb_yds_top10_data.csv',
        'rb_tds': f'{year}_rb_tds_top10_data.csv',
        'rec_yards': f'{year}_rec_yds_top10_data.csv',
        'rec_tds': f'{year}_rec_tds_top10_data.csv',
        'fpa': f'{year}_FPA.csv'
    }
    
    # Check opportunity files
    opp_files = {
        'opportunities': f'{year}_opportunity_data.csv',
        'trends': f'{year}_opportunity_trends.csv'
    }
    
    health_status = {
        'year': year,
        'season_display': season_display,
        'core_data': {},
        'stat_data': {},
        'opportunity_data': {},
        'overall_status': 'healthy'
    }
    
    # Check core files
    for name, filename in core_files.items():
        path = data_dir + filename
        exists = os.path.exists(path)
        health_status['core_data'][name] = {
            'exists': exists,
            'path': path,
            'size': os.path.getsize(path) if exists else 0
        }
        if not exists:
            health_status['overall_status'] = 'incomplete'
    
    # Check stat files
    for name, filename in stat_files.items():
        path = data_dir + filename
        exists = os.path.exists(path)
        health_status['stat_data'][name] = {
            'exists': exists,
            'path': path,
            'size': os.path.getsize(path) if exists else 0
        }
    
    # Check opportunity files
    for name, filename in opp_files.items():
        path = data_dir + filename
        exists = os.path.exists(path)
        health_status['opportunity_data'][name] = {
            'exists': exists,
            'path': path,
            'size': os.path.getsize(path) if exists else 0
        }
    
    logger.info(f"Health check complete for {season_display}: {health_status['overall_status']}")
    return health_status


@celery.task(name='nfl.orchestrator.multi_year_update')
def update_multiple_years(start_year, end_year):
    """
    Update data for multiple years sequentially
    Useful for backfilling historical data
    """
    logger.info(f"Starting multi-year update: {start_year} to {end_year}")
    
    years = range(start_year, end_year + 1)
    results = []
    
    for year in years:
        logger.info(f"Scheduling update for {year}")
        result = update_full_season_data.delay(year)
        results.append({
            'year': year,
            'task_id': result.id
        })
        
        # Add delay between years to avoid overwhelming the system
        time.sleep(5)
    
    return {
        'start_year': start_year,
        'end_year': end_year,
        'years_updated': len(results),
        'results': results,
        'message': f"Multi-year update initiated for {start_year}-{end_year}"
    }


# Convenience wrappers for backwards compatibility
@celery.task(name='nfl.update_all')
def update_all_nfl_data(year):
    """Backwards compatible wrapper for full season update"""
    return update_full_season_data(year)


@celery.task(name='nfl.update_team')
def update_team_data(team, year):
    """Backwards compatible wrapper for single team update"""
    return update_single_team_full(team, year)