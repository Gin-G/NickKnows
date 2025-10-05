"""
NFL Data Celery Tasks Package
Organized task structure for NFL data processing

Task Organization:
- core_data_tasks: Base data loading from nflreadpy
- stat_aggregation_tasks: Statistical calculations and leader boards
- team_analysis_tasks: Team-specific analysis and FPA
- opportunity_tasks: Opportunity tracking and trends
- snap_count_tasks: Snap count processing
- task_orchestrator: High-level workflows and coordination

"""

# Import all task modules to register them with Celery
from . import core_data_tasks
from . import stat_aggregation_tasks
from . import team_analysis_tasks
from . import opportunity_tasks
from . import snap_count_tasks
from . import task_orchestrator

# Export commonly used tasks for easy access
from .core_data_tasks import (
    update_pbp_data,
    update_roster_data,
    update_schedule_data,
    update_player_stats_data,
    update_snap_counts_data,
    update_players_data,
    update_all_base_data,
    check_data_availability
)

from .stat_aggregation_tasks import (
    calculate_qb_yards_leaders,
    calculate_qb_td_leaders,
    calculate_rb_yards_leaders,
    calculate_rb_td_leaders,
    calculate_rec_yards_leaders,
    calculate_rec_td_leaders,
    calculate_all_stat_leaders
)

from .team_analysis_tasks import (
    update_team_schedule,
    update_weekly_team_data,
    process_team_fpa,
    update_all_team_fpa,
    update_single_team_data
)

from .opportunity_tasks import (
    calculate_opportunity_data,
    update_team_opportunity_data
)

from .snap_count_tasks import (
    update_team_snap_counts,
    update_all_teams_snap_counts,
    get_team_snap_summary,
    get_weekly_snap_breakdown,
    check_snap_count_availability
)

from .task_orchestrator import (
    update_full_season_data,
    update_core_data_only,
    update_single_team_full,
    update_current_week_data,
    update_opportunities_only,
    update_snap_counts_only,
    system_health_check,
    update_multiple_years
)

__all__ = [
    # Core data
    'update_pbp_data',
    'update_roster_data',
    'update_schedule_data',
    'update_player_stats_data',
    'update_snap_counts_data',
    'update_players_data',
    'update_all_base_data',
    'check_data_availability',
    
    # Stats
    'calculate_qb_yards_leaders',
    'calculate_qb_td_leaders',
    'calculate_rb_yards_leaders',
    'calculate_rb_td_leaders',
    'calculate_rec_yards_leaders',
    'calculate_rec_td_leaders',
    'calculate_all_stat_leaders',
    
    # Team analysis
    'update_team_schedule',
    'update_weekly_team_data',
    'process_team_fpa',
    'update_all_team_fpa',
    'update_single_team_data',
    
    # Opportunities
    'calculate_opportunity_data',
    'update_team_opportunity_data',
    
    # Snap counts
    'update_team_snap_counts',
    'update_all_teams_snap_counts',
    'get_team_snap_summary',
    'get_weekly_snap_breakdown',
    'check_snap_count_availability',
    
    # Orchestrator
    'update_full_season_data',
    'update_core_data_only',
    'update_single_team_full',
    'update_current_week_data',
    'update_opportunities_only',
    'update_snap_counts_only',
    'system_health_check',
    'update_multiple_years'
]

# Task name mapping for easy reference
TASK_REGISTRY = {
    # Core data tasks
    'core.pbp': 'nfl.core.update_pbp',
    'core.rosters': 'nfl.core.update_rosters',
    'core.schedules': 'nfl.core.update_schedules',
    'core.player_stats': 'nfl.core.update_player_stats',
    'core.snap_counts': 'nfl.core.update_snap_counts',
    'core.players': 'nfl.core.update_players',
    'core.all': 'nfl.core.update_all_base_data',
    'core.check': 'nfl.core.check_data_availability',
    
    # Stat aggregation tasks
    'stats.qb_yards': 'nfl.stats.calculate_qb_yards_leaders',
    'stats.qb_tds': 'nfl.stats.calculate_qb_td_leaders',
    'stats.rb_yards': 'nfl.stats.calculate_rb_yards_leaders',
    'stats.rb_tds': 'nfl.stats.calculate_rb_td_leaders',
    'stats.rec_yards': 'nfl.stats.calculate_rec_yards_leaders',
    'stats.rec_tds': 'nfl.stats.calculate_rec_td_leaders',
    'stats.all': 'nfl.stats.calculate_all_leaders',
    
    # Team tasks
    'team.schedule': 'nfl.team.update_team_schedule',
    'team.weekly_data': 'nfl.team.update_weekly_team_data',
    'team.fpa': 'nfl.team.process_team_fpa',
    'team.all_fpa': 'nfl.team.update_all_team_fpa',
    'team.single': 'nfl.team.update_single_team',
    
    # Opportunity tasks
    'opp.calculate': 'nfl.opportunity.calculate_opportunities',
    'opp.team': 'nfl.opportunity.update_team_opportunities',
    
    # Snap count tasks
    'snaps.team': 'nfl.snaps.update_team_snap_counts',
    'snaps.all': 'nfl.snaps.update_all_teams',
    'snaps.summary': 'nfl.snaps.get_team_summary',
    'snaps.weekly': 'nfl.snaps.get_weekly_breakdown',
    'snaps.check': 'nfl.snaps.check_availability',
    
    # Orchestrator tasks
    'orchestrator.full': 'nfl.orchestrator.update_full_season',
    'orchestrator.core': 'nfl.orchestrator.update_core_only',
    'orchestrator.team': 'nfl.orchestrator.update_single_team_full',
    'orchestrator.week': 'nfl.orchestrator.update_current_week',
    'orchestrator.opportunities': 'nfl.orchestrator.update_opportunities_only',
    'orchestrator.snaps': 'nfl.orchestrator.update_snap_counts_only',
    'orchestrator.health': 'nfl.orchestrator.health_check',
    'orchestrator.multi_year': 'nfl.orchestrator.multi_year_update'
}


def get_task_by_name(short_name):
    """
    Get Celery task by short name
    
    Example:
        task = get_task_by_name('core.pbp')
        task.delay(2024)
    """
    from nickknows import celery
    
    if short_name in TASK_REGISTRY:
        return celery.tasks[TASK_REGISTRY[short_name]]
    else:
        raise KeyError(f"Unknown task: {short_name}")


def list_available_tasks():
    """List all available tasks with their short names"""
    return TASK_REGISTRY