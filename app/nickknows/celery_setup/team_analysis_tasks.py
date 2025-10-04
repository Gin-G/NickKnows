"""
NFL Team Analysis Tasks
Handles team-specific data processing, FPA calculations, and visualizations
"""
from nickknows import celery
import os
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from celery import chain, chord
from celery.utils.log import get_task_logger

logger = get_task_logger(__name__)

SITE_DOMAIN = "https://www.nickknows.net"


def get_data_path(year, data_type):
    """Get standardized data file path"""
    return os.getcwd() + f'/nickknows/nfl/data/{year}_{data_type}.csv'


def get_team_data_path(team, year, data_type):
    """Get team-specific data file path"""
    return os.getcwd() + f'/nickknows/nfl/data/{team}/{year}_{team}_{data_type}.csv'


def format_nfl_season(year):
    """Format NFL season display name"""
    return f"{year-1}-{year} Season"


@celery.task(name='nfl.team.update_team_schedule')
def update_team_schedule(team, year):
    """Update team schedule for specified team and year"""
    season_display = format_nfl_season(year)
    logger.info(f"Updating team schedule for {team} ({season_display})")
    
    try:
        # Ensure team directory exists
        team_dir = os.getcwd() + f'/nickknows/nfl/data/{team}/'
        os.makedirs(team_dir, exist_ok=True)
        
        # Load schedule data
        schedule_path = get_data_path(year, 'schedule')
        if not os.path.exists(schedule_path):
            raise FileNotFoundError(f"Schedule data not found for {year}")
        
        schedule = pd.read_csv(schedule_path, index_col=0)
        
        # Create game links
        url = (f'<a href="{SITE_DOMAIN}/NFL/PbP/' + 
               schedule['game_id'] + '">' + 
               schedule['away_team'] + ' vs. ' + 
               schedule['home_team'] + '</a>')
        schedule['game_id'] = url.astype('string')
        
        # Filter to team games
        home_games = schedule[schedule['home_team'] == team]
        away_games = schedule[schedule['away_team'] == team]
        team_schedule = pd.concat([home_games, away_games])
        
        # Remove unplayed games and sort
        team_schedule = team_schedule.dropna(subset=['away_score'])
        team_schedule = team_schedule.sort_values('week')
        
        # Save team schedule
        output_path = get_team_data_path(team, year, 'schedule')
        team_schedule.to_csv(output_path)
        
        logger.info(f"✅ Team schedule for {team} ({season_display}) saved")
        return f"Successfully updated schedule for {team} ({season_display})"
        
    except Exception as e:
        logger.error(f"❌ Error updating team schedule for {team} ({season_display}): {str(e)}")
        raise


@celery.task(name='nfl.team.update_weekly_team_data')
def update_weekly_team_data(team, year):
    """Update weekly team data for opponents faced"""
    season_display = format_nfl_season(year)
    logger.info(f"Processing weekly data for {team} ({season_display})")
    
    try:
        # Load required data
        schedule_path = get_team_data_path(team, year, 'schedule')
        roster_path = get_data_path(year, 'rosters')
        stats_path = get_data_path(year, 'weekly_data')
        
        team_schedule = pd.read_csv(schedule_path, index_col=0)
        roster_data = pd.read_csv(roster_path, index_col=0)
        player_stats = pd.read_csv(stats_path, index_col=0)
        
        # Identify opponents
        team_schedule['is_home'] = team_schedule['home_team'].str.contains(team)
        opponents = pd.concat([
            team_schedule[team_schedule['is_home'] == True][['away_team', 'week']],
            team_schedule[team_schedule['is_home'] == False][['home_team', 'week']]
        ])
        opponents['opponent'] = opponents['away_team'].fillna('') + opponents['home_team'].fillna('')
        opponents = opponents.sort_values('week')
        
        # Collect opponent player stats for each week
        weekly_team_data = pd.DataFrame()
        
        for _, row in opponents.iterrows():
            week = row['week']
            opponent = row['opponent']
            
            # Get opponent roster
            opp_roster = roster_data[roster_data['team'] == opponent]
            opp_players = opp_roster['player_name'].tolist()
            
            # Get opponent player stats for that week
            week_stats = player_stats[
                (player_stats['player_display_name'].isin(opp_players)) &
                (player_stats['week'] == week) &
                (player_stats['recent_team'] == opponent)
            ]
            
            weekly_team_data = pd.concat([weekly_team_data, week_stats])
        
        # Save team data
        output_path = get_team_data_path(team, year, 'data')
        weekly_team_data.to_csv(output_path)
        
        logger.info(f"✅ Weekly team data for {team} ({season_display}) saved")
        return f"Successfully processed weekly data for {team} ({season_display})"
        
    except Exception as e:
        logger.error(f"❌ Error processing weekly team data for {team} ({season_display}): {str(e)}")
        raise


@celery.task(name='nfl.team.process_team_fpa')
def process_team_fpa(team, year):
    """Process team FPA data and generate visualizations"""
    season_display = format_nfl_season(year)
    logger.info(f"Processing FPA for {team} ({season_display})")
    
    try:
        # Load team data
        data_path = get_team_data_path(team, year, 'data')
        team_data = pd.read_csv(data_path, index_col=0)
        
        # Group by week and position to get totals per week
        weekly_position_totals = team_data.groupby(['week', 'position'])['fantasy_points_ppr'].sum().reset_index()
        
        # Generate position plots
        generate_team_fpa_plots(team, team_data, year)
        
        # Calculate mean fantasy points against per position
        fpa_stats = {}
        for position in ['QB', 'RB', 'WR', 'TE']:
            pos_data = weekly_position_totals[weekly_position_totals['position'] == position]
            if len(pos_data) > 0:
                fpa_stats[position] = pos_data.groupby('week')['fantasy_points_ppr'].first().mean()
            else:
                fpa_stats[position] = 0
        
        result = {
            'Team Name': team,
            'QB': fpa_stats['QB'],
            'RB': fpa_stats['RB'],
            'WR': fpa_stats['WR'],
            'TE': fpa_stats['TE']
        }
        
        logger.info(f"✅ FPA processed for {team} ({season_display})")
        return result
        
    except Exception as e:
        logger.error(f"❌ Error processing FPA for {team} ({season_display}): {str(e)}")
        raise


def generate_team_fpa_plots(team, team_data, year):
    """Generate FPA visualization plots for a team"""
    positions = {
        'QB': team_data[team_data['position'] == 'QB'],
        'RB': team_data[team_data['position'] == 'RB'],
        'WR': team_data[team_data['position'] == 'WR'],
        'TE': team_data[team_data['position'] == 'TE']
    }
    
    # Plot configuration
    BAR_HEIGHT = 0.6
    MIN_HEIGHT = 4
    MAX_HEIGHT = 12
    WIDTH = 10
    
    # Create plots directory
    plots_dir = f'/NickKnows/app/nickknows/static/images/{team}/'
    os.makedirs(plots_dir, exist_ok=True)
    
    for pos, data in positions.items():
        if len(data) == 0:
            continue
        
        num_players = len(data)
        fig_height = max(MIN_HEIGHT, min(MAX_HEIGHT, num_players * 0.8))
        
        fig, ax = plt.subplots(figsize=(WIDTH, fig_height))
        
        data.plot.barh(
            x='player_display_name',
            y='fantasy_points_ppr',
            ylabel='',
            ax=ax
        )
        
        plt.title(f'{team} vs {pos}s Fantasy Points ({year})')
        plt.yticks(fontsize=8)
        plt.margins(y=BAR_HEIGHT / (2 * num_players))
        plt.subplots_adjust(left=0.3)
        
        ax.set_ylim(-0.5, num_players - 0.5)
        
        for bar in ax.containers[0]:
            bar.set_height(BAR_HEIGHT)
        
        plt.grid(axis='x', linestyle='--', alpha=0.7)
        
        plt.savefig(
            f'{plots_dir}{team}_{pos}_FPA_{year}.png',
            bbox_inches='tight',
            dpi=100
        )
        plt.close(fig)
        
        logger.info(f"Generated {pos} FPA plot for {team} ({year})")


@celery.task(name='nfl.team.update_all_team_fpa')
def update_all_team_fpa(year):
    """Update FPA data for all teams"""
    season_display = format_nfl_season(year)
    logger.info(f"Starting FPA update for all teams ({season_display})")
    
    teams = [
        'ARI', 'ATL', 'BAL', 'BUF', 'CAR', 'CHI', 'CIN', 'CLE',
        'DAL', 'DEN', 'DET', 'GB', 'HOU', 'IND', 'JAX', 'KC',
        'LA', 'LAC', 'LV', 'MIA', 'MIN', 'NE', 'NO', 'NYG',
        'NYJ', 'PHI', 'PIT', 'SEA', 'SF', 'TB', 'TEN', 'WAS'
    ]
    
    # Create chains for each team
    team_chains = []
    for team in teams:
        team_chains.append(
            chain(
                update_team_schedule.si(team, year),
                update_weekly_team_data.si(team, year),
                process_team_fpa.si(team, year)
            )
        )
    
    # Execute in parallel and aggregate results
    chord(team_chains)(save_fpa_summary.s(year))
    
    logger.info(f"Scheduled FPA updates for all teams ({season_display})")
    return f"FPA update scheduled for all teams ({season_display})"


@celery.task(name='nfl.team.save_fpa_summary')
def save_fpa_summary(results, year):
    """Save aggregated FPA data for all teams"""
    season_display = format_nfl_season(year)
    logger.info(f"Saving FPA summary for all teams ({season_display})")
    
    try:
        fpa_path = get_data_path(year, 'FPA')
        df = pd.DataFrame(results)
        df.to_csv(fpa_path)
        
        logger.info(f"FPA data for {len(results)} teams saved for {season_display}")
        return f"Updated FPA data for {len(results)} teams ({season_display})"
        
    except Exception as e:
        logger.error(f"Error saving FPA summary for {season_display}: {str(e)}")
        raise


@celery.task(name='nfl.team.update_single_team')
def update_single_team_data(team, year):
    """Update all data for a single team"""
    season_display = format_nfl_season(year)
    logger.info(f"Updating all data for {team} ({season_display})")
    
    try:
        # Update team schedule
        update_team_schedule(team, year)
        
        # Update weekly team data
        update_weekly_team_data(team, year)
        
        # Process FPA
        fpa_result = process_team_fpa(team, year)
        
        logger.info(f"Successfully updated all data for {team} ({season_display})")
        return {
            'team': team,
            'year': year,
            'season_display': season_display,
            'fpa': fpa_result
        }
        
    except Exception as e:
        logger.error(f"Error updating {team} ({season_display}): {str(e)}")
        raise