"""
NFL Opportunity Tracking Tasks
Handles calculation and analysis of player opportunities (targets, carries, etc.)
FIXED: Compatible with Polars DataFrames from nflreadpy
"""
from nickknows import celery
import os
import pandas as pd
import numpy as np
from collections import defaultdict
from celery.utils.log import get_task_logger

logger = get_task_logger(__name__)


def get_data_path(year, data_type):
    """Get standardized data file path"""
    return os.getcwd() + f'/nickknows/nfl/data/{year}_{data_type}.csv'


def format_nfl_season(year):
    """Format NFL season display name"""
    return f"{year-1}-{year} Season"


@celery.task(name='nfl.opportunity.calculate_opportunities')
def calculate_opportunity_data(year):
    """Calculate opportunity metrics from PBP data"""
    season_display = format_nfl_season(year)
    logger.info(f"Calculating opportunity data for {season_display}")
    
    try:
        # Load PBP data
        import nflreadpy as nfl
        
        # nflreadpy returns Polars DataFrames, convert to Pandas
        if year >= 2024:
            pbp_data = nfl.load_pbp(seasons=[year])
        else:
            pbp_data = nfl.load_pbp(seasons=[year])
        
        # Convert Polars to Pandas
        if hasattr(pbp_data, 'to_pandas'):
            pbp_data = pbp_data.to_pandas()
        # Otherwise it's already a Pandas DataFrame
        
        logger.info(f"Loaded {len(pbp_data)} PBP records")
        
        # Load roster data for player info
        try:
            roster_data = nfl.load_rosters_weekly(seasons=[year])
            # Convert Polars to Pandas
            if hasattr(roster_data, 'to_pandas'):
                roster_data = roster_data.to_pandas()
            has_roster = True
        except Exception as e:
            logger.warning(f"Could not load roster data: {e}")
            roster_data = pd.DataFrame()
            has_roster = False
        
        # Filter to regular season (now using Pandas syntax)
        reg_season = pbp_data[pbp_data['season_type'] == 'REG'].copy()
        logger.info(f"Processing {len(reg_season)} regular season plays")
        
        # Process opportunities by week
        opportunity_records = []
        weeks = sorted(reg_season['week'].unique())
        
        for week in weeks:
            week_data = reg_season[reg_season['week'] == week]
            week_opps = process_week_opportunities(week_data, week, year)
            opportunity_records.extend(week_opps)
            logger.debug(f"Processed week {week}: {len(week_opps)} opportunity records")
        
        # Convert to DataFrame
        opportunity_df = pd.DataFrame(opportunity_records)
        
        # Add roster information
        if has_roster and len(roster_data) > 0:
            opportunity_df = add_roster_info(opportunity_df, roster_data)
        
        # Save opportunity data
        output_path = get_data_path(year, 'opportunity_data')
        opportunity_df.to_csv(output_path, index=False)
        
        logger.info(f"✅ Opportunity data saved: {len(opportunity_df)} records")
        
        # Calculate trends
        trend_data = calculate_opportunity_trends(opportunity_df)
        trend_path = get_data_path(year, 'opportunity_trends')
        trend_data.to_csv(trend_path, index=False)
        
        logger.info(f"✅ Trend data saved: {len(trend_data)} records")
        
        return f"Successfully calculated opportunity data for {season_display}"
        
    except Exception as e:
        logger.error(f"❌ Error calculating opportunities for {season_display}: {str(e)}")
        import traceback
        logger.error(traceback.format_exc())
        raise


def process_week_opportunities(week_data, week, year):
    """Process opportunities for a single week"""
    opportunities = defaultdict(lambda: {
        'player_id': '',
        'week': week,
        'season': year,
        'targets': 0,
        'red_zone_targets': 0,
        'end_zone_targets': 0,
        'carries': 0,
        'red_zone_carries': 0,
        'goal_line_carries': 0,
        'air_yards': 0,
        'touches': 0,
        'goal_line_touches': 0,
        'third_down_targets': 0,
        'deep_targets': 0,
        'short_targets': 0,
        'team': ''
    })
    
    # Team totals for share calculations
    team_totals = defaultdict(lambda: {
        'total_targets': 0,
        'total_carries': 0
    })
    
    # Process each play
    for _, play in week_data.iterrows():
        play_type = play.get('play_type', '')
        down = play.get('down', 0)
        yardline_100 = play.get('yardline_100', 100)
        air_yards = play.get('air_yards', 0) if pd.notna(play.get('air_yards')) else 0
        
        # PASSING OPPORTUNITIES
        if play_type == 'pass':
            receiver_id = play.get('receiver_player_id')
            posteam = play.get('posteam')
            
            if pd.notna(receiver_id):
                opp = opportunities[receiver_id]
                opp['player_id'] = receiver_id
                opp['targets'] += 1
                opp['touches'] += 1
                opp['air_yards'] += air_yards
                opp['team'] = posteam
                
                if pd.notna(posteam):
                    team_totals[posteam]['total_targets'] += 1
                
                # Situational opportunities
                if yardline_100 <= 20:
                    opp['red_zone_targets'] += 1
                if yardline_100 <= 10:
                    opp['end_zone_targets'] += 1
                    opp['goal_line_touches'] += 1
                if down == 3:
                    opp['third_down_targets'] += 1
                if air_yards >= 20:
                    opp['deep_targets'] += 1
                elif air_yards < 10:
                    opp['short_targets'] += 1
        
        # RUSHING OPPORTUNITIES
        elif play_type == 'run':
            rusher_id = play.get('rusher_player_id')
            posteam = play.get('posteam')
            
            if pd.notna(rusher_id):
                opp = opportunities[rusher_id]
                opp['player_id'] = rusher_id
                opp['carries'] += 1
                opp['touches'] += 1
                opp['team'] = posteam
                
                if pd.notna(posteam):
                    team_totals[posteam]['total_carries'] += 1
                
                # Situational opportunities
                if yardline_100 <= 20:
                    opp['red_zone_carries'] += 1
                if yardline_100 <= 5:
                    opp['goal_line_carries'] += 1
                    opp['goal_line_touches'] += 1
    
    # Calculate shares
    records = []
    for player_id, stats in opportunities.items():
        team = stats['team']
        
        if team and team in team_totals:
            total_targets = team_totals[team]['total_targets']
            total_carries = team_totals[team]['total_carries']
            
            stats['target_share'] = (stats['targets'] / total_targets * 100) if total_targets > 0 else 0
            stats['carry_share'] = (stats['carries'] / total_carries * 100) if total_carries > 0 else 0
        else:
            stats['target_share'] = 0
            stats['carry_share'] = 0
        
        records.append(stats)
    
    return records


def add_roster_info(opportunity_df, roster_data):
    """Add roster information to opportunity data"""
    try:
        # nflreadpy uses 'gsis_id' and 'full_name' instead of 'player_id' and 'player_name'
        # First, rename the roster columns to match what we expect
        roster_rename = {
            'gsis_id': 'player_id',
            'full_name': 'player_name'
        }
        
        # Only rename columns that exist
        roster_rename_filtered = {k: v for k, v in roster_rename.items() if k in roster_data.columns}
        roster_data = roster_data.rename(columns=roster_rename_filtered)
        
        # Get unique player info from roster (most recent info)
        player_info = roster_data.groupby('player_id').agg({
            'player_name': 'first',
            'position': 'first',
            'team': 'first'
        }).reset_index()
        
        logger.info(f"Extracted info for {len(player_info)} players from roster")
        
        # Merge with opportunity data
        # First, save the original team column
        opportunity_df['opp_team'] = opportunity_df['team']
        
        # Merge on player_id
        opportunity_df = opportunity_df.merge(
            player_info, 
            on='player_id', 
            how='left',
            suffixes=('', '_roster')
        )
        
        # Set player_display_name
        opportunity_df['player_display_name'] = opportunity_df['player_name'].fillna(opportunity_df['player_id'])
        
        # Use roster team if available, otherwise use opportunity team
        opportunity_df['team'] = opportunity_df['team_roster'].fillna(opportunity_df['opp_team'])
        
        # Clean up temporary columns
        opportunity_df.drop(['opp_team', 'team_roster'], axis=1, inplace=True, errors='ignore')
        
        # Fill missing positions with 'Unknown'
        if 'position' in opportunity_df.columns:
            opportunity_df['position'] = opportunity_df['position'].fillna('Unknown')
        else:
            opportunity_df['position'] = 'Unknown'
        
        logger.info(f"Successfully added roster info to {len(opportunity_df)} opportunity records")
        
        # Log some examples for debugging
        sample = opportunity_df[['player_id', 'player_name', 'player_display_name', 'position', 'team']].head(10)
        logger.info(f"Sample merged data:\n{sample}")
        
    except Exception as e:
        logger.error(f"Error adding roster info: {e}")
        import traceback
        logger.error(traceback.format_exc())
        
        # Add default columns if merge failed
        if 'player_name' not in opportunity_df.columns:
            opportunity_df['player_name'] = opportunity_df['player_id']
        if 'player_display_name' not in opportunity_df.columns:
            opportunity_df['player_display_name'] = opportunity_df['player_id']
        if 'position' not in opportunity_df.columns:
            opportunity_df['position'] = 'Unknown'
    
    return opportunity_df


def calculate_opportunity_trends(opportunity_df, min_weeks=2):
    """Calculate trend analysis from opportunity data"""
    logger.info(f"Calculating trends (min {min_weeks} weeks)")
    
    trend_records = []
    
    # Metrics to analyze
    metrics = [
        'targets', 'carries', 'touches', 'target_share', 'carry_share',
        'red_zone_targets', 'red_zone_carries', 'goal_line_touches',
        'deep_targets', 'short_targets'
    ]
    
    for player_id, player_data in opportunity_df.groupby('player_id'):
        player_data = player_data.sort_values('week')
        
        if len(player_data) < min_weeks:
            continue
        
        # Get player info - prioritize player_display_name, then player_name, then player_id
        if 'player_display_name' in player_data.columns and pd.notna(player_data['player_display_name'].iloc[0]):
            player_name = player_data['player_display_name'].iloc[0]
        elif 'player_name' in player_data.columns and pd.notna(player_data['player_name'].iloc[0]):
            player_name = player_data['player_name'].iloc[0]
        else:
            player_name = player_id
        
        # Get position - default to 'Unknown' if missing
        if 'position' in player_data.columns and pd.notna(player_data['position'].iloc[0]):
            position = player_data['position'].iloc[0]
        else:
            position = 'Unknown'
        
        # Get team
        if 'team' in player_data.columns and pd.notna(player_data['team'].iloc[0]):
            team = player_data['team'].iloc[0]
        else:
            team = 'Unknown'
        
        trend_record = {
            'player_id': player_id,
            'player_name': player_name,
            'position': position,
            'team': team,
            'weeks_played': len(player_data),
            'latest_week': player_data['week'].max()
        }
        
        # Calculate stats for each metric
        for metric in metrics:
            if metric in player_data.columns:
                values = player_data[metric].values
                
                # Basic stats
                trend_record[f'{metric}_avg'] = np.mean(values)
                trend_record[f'{metric}_latest'] = values[-1] if len(values) > 0 else 0
                trend_record[f'{metric}_max'] = np.max(values)
                
                # Trend (recent vs early)
                if len(values) >= 2:
                    if len(values) >= 3:
                        recent_avg = np.mean(values[-2:])
                        early_avg = np.mean(values[:-2])
                    else:
                        recent_avg = values[-1]
                        early_avg = values[0]
                    
                    trend_pct = ((recent_avg - early_avg) / max(early_avg, 0.1) * 100) if early_avg > 0 else 0
                    trend_record[f'{metric}_trend'] = trend_pct
                else:
                    trend_record[f'{metric}_trend'] = 0
                
                # Consistency (coefficient of variation)
                if np.mean(values) > 0:
                    trend_record[f'{metric}_consistency'] = (np.std(values) / np.mean(values)) * 100
                else:
                    trend_record[f'{metric}_consistency'] = 0
        
        trend_records.append(trend_record)
    
    logger.info(f"Calculated trends for {len(trend_records)} players")
    
    return pd.DataFrame(trend_records)


@celery.task(name='nfl.opportunity.update_team_opportunities')
def update_team_opportunity_data(team, year):
    """Update opportunity data for a specific team"""
    season_display = format_nfl_season(year)
    logger.info(f"Updating opportunity data for {team} ({season_display})")
    
    try:
        # Load full opportunity data
        opp_path = get_data_path(year, 'opportunity_data')
        trend_path = get_data_path(year, 'opportunity_trends')
        
        if not os.path.exists(opp_path):
            # Trigger full calculation if not available
            calculate_opportunity_data(year)
            return f"Triggered opportunity calculation for {season_display}"
        
        # Load and filter to team
        opp_data = pd.read_csv(opp_path)
        trend_data = pd.read_csv(trend_path)
        
        team_opps = opp_data[opp_data['team'] == team]
        team_trends = trend_data[trend_data['team'] == team]
        
        # Save team-specific data
        team_dir = os.getcwd() + f'/nickknows/nfl/data/{team}/'
        os.makedirs(team_dir, exist_ok=True)
        
        team_opps.to_csv(f'{team_dir}{year}_{team}_opportunities.csv', index=False)
        team_trends.to_csv(f'{team_dir}{year}_{team}_opportunity_trends.csv', index=False)
        
        logger.info(f"✅ Team opportunity data saved for {team} ({season_display})")
        return f"Updated opportunity data for {team} ({season_display})"
        
    except Exception as e:
        logger.error(f"❌ Error updating opportunities for {team} ({season_display}): {str(e)}")
        raise