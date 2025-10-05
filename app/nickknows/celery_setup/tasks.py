"""
Backwards Compatibility Layer for NFL Tasks
This file maintains the old import structure while using the new modular tasks

Legacy code can still import from:
    from ..celery_setup.tasks import update_pbp_data

New code should import from:
    from ..celery_setup.core_data_tasks import update_pbp_data
    OR
    from ..celery_setup.tasks import update_pbp_data  (works via this file)
"""

# Import all tasks from new modular structure
from .core_data_tasks import (
    update_pbp_data,
    update_roster_data,
    update_schedule_data,
    update_player_stats_data,
    update_snap_counts_data,
    update_players_data,
    update_all_base_data,
    check_data_availability,
    get_available_years,
    get_selected_year,
    format_nfl_season,
    get_data_path
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
    save_fpa_summary,
    update_single_team_data
)

from .opportunity_tasks import (
    calculate_opportunity_data,
    update_team_opportunity_data
)

from .snap_count_tasks import (
    update_team_snap_counts,
    update_all_teams_snap_counts,
    get_snap_count_summary,
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

# Backwards compatibility aliases for old function names
# These allow old code to keep working without changes

# Old PBP function name
update_PBP_data = update_pbp_data

# Old schedule function name  
update_sched_data = update_schedule_data

# Old weekly data function name (now uses player_stats)
update_week_data = update_player_stats_data

# Old top10 function names -> new calculate leaders
update_qb_yards_top10 = calculate_qb_yards_leaders
update_qb_tds_top10 = calculate_qb_td_leaders
update_rb_yards_top10 = calculate_rb_yards_leaders
update_rb_tds_top10 = calculate_rb_td_leaders
update_rec_yds_top10 = calculate_rec_yards_leaders
update_rec_tds_top10 = calculate_rec_td_leaders

# Old FPA function name
update_fpa_data = save_fpa_summary
process_team_data = process_team_fpa

# Old snap count function name
update_snap_count_data = update_team_snap_counts

# Old opportunity function name
update_opportunity_data = calculate_opportunity_data

# Export everything for backwards compatibility
__all__ = [
    # New standardized names (preferred)
    'update_pbp_data',
    'update_roster_data',
    'update_schedule_data',
    'update_player_stats_data',
    'update_snap_counts_data',
    'update_players_data',
    'update_all_base_data',
    'check_data_availability',
    'calculate_qb_yards_leaders',
    'calculate_qb_td_leaders',
    'calculate_rb_yards_leaders',
    'calculate_rb_td_leaders',
    'calculate_rec_yards_leaders',
    'calculate_rec_td_leaders',
    'calculate_all_stat_leaders',
    'update_team_schedule',
    'update_weekly_team_data',
    'process_team_fpa',
    'update_all_team_fpa',
    'save_fpa_summary',
    'update_single_team_data',
    'calculate_opportunity_data',
    'update_team_opportunity_data',
    'update_team_snap_counts',
    'update_all_teams_snap_counts',
    'get_team_snap_summary',
    'get_weekly_snap_breakdown',
    'check_snap_count_availability',
    'update_full_season_data',
    'update_core_data_only',
    'update_single_team_full',
    'update_current_week_data',
    'update_opportunities_only',
    'update_snap_counts_only',
    'system_health_check',
    'update_multiple_years',
    
    # Old names (for backwards compatibility)
    'update_PBP_data',
    'update_sched_data',
    'update_week_data',
    'update_qb_yards_top10',
    'update_qb_tds_top10',
    'update_rb_yards_top10',
    'update_rb_tds_top10',
    'update_rec_yds_top10',
    'update_rec_tds_top10',
    'update_fpa_data',
    'process_team_data',
    'update_snap_count_data',
    'update_opportunity_data',
    
    # Utility functions
    'get_available_years',
    'get_selected_year',
    'format_nfl_season',
    'get_data_path'
]