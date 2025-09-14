from nickknows import celery
import os
import time
import nfl_data_py as nfl
import pandas as pd
import numpy as np
from flask import flash, request
import matplotlib.pyplot as plt
from celery.utils.log import get_task_logger
from datetime import datetime

logger = get_task_logger(__name__)

# Add at top of file with other constants
SITE_DOMAIN = "https://www.nickknows.net"

current_season = 2025

def get_available_years():
    """Get available NFL years from 2020 to current available year"""
    return list(range(2020, current_season + 1 ))

def get_selected_year():
    """Get selected year from context or default to latest available"""
    available_years = get_available_years()
    try:
        from flask import request
        selected = request.args.get('year', max(available_years), type=int)
        return selected if selected in available_years else max(available_years)
    except:
        return max(available_years)  # This will be 2024

def format_nfl_season(year):
    """Format NFL season as 'YYYY-YYYY Season' (e.g., '2024-2025 Season')"""
    return f"{year-1}-{year} Season"

def is_data_likely_available(year):
    """Check if NFL data is likely to be available for a given year based on nfl_data_py capabilities"""
    current_year = datetime.now().year
    current_month = datetime.now().month
    
    # Per GitHub issue #144: https://github.com/nflverse/nfl_data_py/issues/144
    known_unavailable = [2025, 2026]
    if year in known_unavailable:
        return False
    
    if year < current_year:
        return year >= 1999
    
    if year == current_year:
        if current_month >= 10: 
            return True
        elif current_month == 9:  
            return datetime.now().day >= 15
        else:  
            return False
    return False

@celery.task()
def update_PBP_data(year=None):
    """Update play-by-play data for specified year"""
    if year is None:
        year = get_selected_year()
    
    file_path = os.getcwd() + '/nickknows/nfl/data/' + str(year) + '_pbp_data.csv'
    season_display = format_nfl_season(year)
    logger.info(f"Updating PBP data for {season_display}")
    
    if year == 2024:
        pbp_data = nfl.import_pbp_data([year], include_participation=False)
    else:
        pbp_data = nfl.import_pbp_data([year])
    pbp_data.to_csv(file_path)
    logger.info(f"PBP data for {season_display} saved to {file_path}")

@celery.task()
def update_roster_data(year=None):
    """Update roster data for specified year"""
    if year is None:
        year = get_selected_year()
        
    rfile_path = os.getcwd() + '/nickknows/nfl/data/' + str(year) + '_rosters.csv'
    season_display = format_nfl_season(year)
    logger.info(f"Updating roster data for {season_display}")
    
    roster_data = nfl.import_weekly_rosters([year])
    roster_data.to_csv(rfile_path)
    logger.info(f"Roster data for {season_display} saved to {rfile_path}")

@celery.task()
def update_sched_data(year=None):
    """Update schedule data for specified year"""
    if year is None:
        year = get_selected_year()
        
    scfile_path = os.getcwd() + '/nickknows/nfl/data/' + str(year) + '_schedule.csv'
    season_display = format_nfl_season(year)
    logger.info(f"Updating schedule data for {season_display}")
    
    schedule = nfl.import_schedules([year])
    schedule.to_csv(scfile_path)
    logger.info(f"Schedule data for {season_display} saved to {scfile_path}")

@celery.task()
def update_week_data(year=None):
    """Update weekly data for specified year"""
    if year is None:
        year = get_selected_year()
        
    wefile_path = os.getcwd() + '/nickknows/nfl/data/' + str(year) + '_weekly_data.csv'
    season_display = format_nfl_season(year)
    logger.info(f"Updating weekly data for {season_display}")
    
    weekly_data = nfl.import_weekly_data([year])
    weekly_data.to_csv(wefile_path)
    logger.info(f"Weekly data for {season_display} saved to {wefile_path}")

@celery.task()
def update_qb_yards_top10(year=None):
    """Update QB yards top 10 for specified year"""
    if year is None:
        year = get_selected_year()
        
    season_display = format_nfl_season(year)
    qb10file_path = os.getcwd() + '/nickknows/nfl/data/' + str(year) + '_qb_yards_top10_data.csv'
    file_path = os.getcwd() + '/nickknows/nfl/data/' + str(year) + '_pbp_data.csv'
    
    if os.path.exists(file_path):
        pbp_data = pd.read_csv(file_path, index_col=0)
    else:
        update_PBP_data.delay(year)
        return f"PBP data for {season_display} not found, updating in background"
        
    rfile_path = os.getcwd() + '/nickknows/nfl/data/' + str(year) + '_rosters.csv'
    if os.path.exists(rfile_path):
        roster_data = pd.read_csv(rfile_path, index_col=0)
    else:
        update_roster_data.delay(year)
        return f"Roster data for {season_display} not found, updating in background"
        
    try:
        # Filter for regular season passing plays
        pbp_data = pbp_data[
            (pbp_data["season_type"] == "REG") &
            (pbp_data["two_point_attempt"] == False) &
            (pbp_data["play_type"] == "pass")
        ]

        # Merge with roster data
        pbp_data_pass = pbp_data.merge(
            roster_data[["player_id", "player_name"]], 
            left_on="passer_player_id", 
            right_on="player_id",
            how='inner'
        )

        # Drop duplicate plays to ensure unique play counting
        pbp_data_pass_unique = pbp_data_pass.drop_duplicates(subset=['game_id', 'play_id'])

        # Group and aggregate passing yards
        pass_agg = pbp_data_pass_unique.groupby(
            ["player_name"], 
            as_index=False
        ).agg({"passing_yards": "sum"})

        # Sort and format
        pass_agg.sort_values(
            by=['passing_yards'], 
            inplace=True, 
            ascending=False
        )

        pass_agg.rename(
            columns={
                'player_name': 'Player Name',
                'passing_yards': "Total Passing Yards"
            }, 
            inplace=True
        )

        # Get top 10
        pass_agg = pass_agg.head(10)

        # Save to CSV
        pass_agg.to_csv(qb10file_path)
        logger.info(f"QB yards top 10 for {season_display} saved to {qb10file_path}")
    except Exception as e:
        logger.error(f"Error updating QB yards top 10 for {season_display}: {str(e)}")
        update_PBP_data.delay(year)
        update_roster_data.delay(year)

@celery.task()
def update_qb_tds_top10(year=None):
    """Update QB TDs top 10 for specified year"""
    if year is None:
        year = get_selected_year()
        
    qbtd10file_path = os.getcwd() + '/nickknows/nfl/data/' + str(year) + '_qb_tds_top10_data.csv'
    file_path = os.getcwd() + '/nickknows/nfl/data/' + str(year) + '_pbp_data.csv'
    
    if os.path.exists(file_path):
        pbp_data = pd.read_csv(file_path, index_col=0)
    else:
        update_PBP_data.delay(year)
        return f"PBP data for {year} not found, updating in background"
        
    rfile_path = os.getcwd() + '/nickknows/nfl/data/' + str(year) + '_rosters.csv'
    if os.path.exists(rfile_path):
        roster_data = pd.read_csv(rfile_path, index_col=0)
    else:
        update_roster_data.delay(year)
        return f"Roster data for {year} not found, updating in background"
        
    try:
        # Filter for regular season passing touchdowns
        pass_td_data = pbp_data[
            (pbp_data["season_type"] == "REG") &
            (pbp_data["two_point_attempt"] == False) &
            (pbp_data["play_type"] == "pass") &
            (pbp_data["pass_touchdown"] == 1)
        ]

        # Merge with roster data
        pass_td_data = pass_td_data.merge(
            roster_data[["player_id", "player_name"]], 
            left_on="passer_player_id", 
            right_on="player_id",
            how='inner'
        )

        # Drop duplicate plays to count unique touchdowns
        pass_td_data_unique = pass_td_data.drop_duplicates(subset=['game_id', 'play_id'])

        # Group and count unique touchdowns
        pass_td_agg = pass_td_data_unique.groupby(
            ["player_name"], 
            as_index=False
        )["pass_touchdown"].count()

        # Sort and format
        pass_td_agg.sort_values(
            by=['pass_touchdown'], 
            inplace=True, 
            ascending=False
        )

        pass_td_agg.rename(
            columns={
                'player_name': 'Player Name',
                "pass_touchdown": "Total Passing TD's"
            }, 
            inplace=True
        )

        # Get top 10
        pass_td_agg = pass_td_agg.head(10)
        pass_td_agg.to_csv(qbtd10file_path)
        logger.info(f"QB TDs top 10 for {year} saved to {qbtd10file_path}")
    except Exception as e:
        logger.error(f"Error updating QB TDs top 10 for {year}: {str(e)}")
        update_PBP_data.delay(year)
        update_roster_data.delay(year)

@celery.task()
def update_rb_yards_top10(year=None):
    """Update RB yards top 10 for specified year"""
    if year is None:
        year = get_selected_year()
        
    rbyds10 = os.getcwd() + '/nickknows/nfl/data/' + str(year) + '_rb_yds_top10_data.csv'
    file_path = os.getcwd() + '/nickknows/nfl/data/' + str(year) + '_pbp_data.csv'
    
    if os.path.exists(file_path):
        pbp_data = pd.read_csv(file_path, index_col=0)
    else:
        update_PBP_data.delay(year)
        return f"PBP data for {year} not found, updating in background"
        
    rfile_path = os.getcwd() + '/nickknows/nfl/data/' + str(year) + '_rosters.csv'
    if os.path.exists(rfile_path):
        roster_data = pd.read_csv(rfile_path, index_col=0)
    else:
        update_roster_data.delay(year)
        return f"Roster data for {year} not found, updating in background"
        
    try:
        # Filter for regular season rushing plays
        pbp_data = pbp_data[
            (pbp_data["season_type"] == "REG") &
            (pbp_data["two_point_attempt"] == False) &
            (pbp_data["play_type"] == "run")
        ]

        # Merge with roster data
        pbp_data_rush = pbp_data.merge(
            roster_data[["player_id", "player_name"]], 
            left_on="rusher_player_id", 
            right_on="player_id",
            how='inner'
        )

        # Drop duplicate plays
        pbp_data_rush_unique = pbp_data_rush.drop_duplicates(subset=['game_id', 'play_id'])

        # Group and aggregate rushing yards
        rush_yds_agg = pbp_data_rush_unique.groupby(
            ["player_name"], 
            as_index=False
        ).agg({"rushing_yards": "sum"})

        # Sort and format
        rush_yds_agg.sort_values(
            by=['rushing_yards'], 
            inplace=True, 
            ascending=False
        )

        rush_yds_agg.rename(
            columns={
                'player_name': 'Player Name',
                'rushing_yards': "Total Rushing Yards"
            }, 
            inplace=True
        )

        # Get top 10
        rush_yds_agg = rush_yds_agg.head(10)

        # Save to CSV
        rush_yds_agg.to_csv(rbyds10)
        logger.info(f"RB yards top 10 for {year} saved to {rbyds10}")
    except Exception as e:
        logger.error(f"Error updating RB yards top 10 for {year}: {str(e)}")
        update_PBP_data.delay(year)
        update_roster_data.delay(year)

@celery.task()
def update_rb_tds_top10(year=None):
    """Update RB TDs top 10 for specified year"""
    if year is None:
        year = get_selected_year()
        
    rbtds10 = os.getcwd() + '/nickknows/nfl/data/' + str(year) + '_rb_tds_top10_data.csv'
    file_path = os.getcwd() + '/nickknows/nfl/data/' + str(year) + '_pbp_data.csv'
    
    if os.path.exists(file_path):
        pbp_data = pd.read_csv(file_path, index_col=0)
    else:
        update_PBP_data.delay(year)
        return f"PBP data for {year} not found, updating in background"
        
    rfile_path = os.getcwd() + '/nickknows/nfl/data/' + str(year) + '_rosters.csv'
    if os.path.exists(rfile_path):
        roster_data = pd.read_csv(rfile_path, index_col=0)
    else:
        update_roster_data.delay(year)
        return f"Roster data for {year} not found, updating in background"
        
    try:
        # Filter for regular season rushing plays
        pbp_data = pbp_data[
            (pbp_data["season_type"] == "REG") &
            (pbp_data["two_point_attempt"] == False) &
            (pbp_data["play_type"] == "run") &
            (pbp_data["rush_touchdown"] == 1)  # Only count actual touchdowns
        ]

        # Merge with roster data
        pbp_data_rush = pbp_data.merge(
            roster_data[["player_id", "player_name"]], 
            left_on="rusher_player_id", 
            right_on="player_id",
            how='inner'
        )

        # Drop duplicate plays
        pbp_data_rush_unique = pbp_data_rush.drop_duplicates(subset=['game_id', 'play_id'])

        # Group and count unique touchdowns
        rush_td_agg = pbp_data_rush_unique.groupby(
            ["player_name"], 
            as_index=False
        )["rush_touchdown"].count()  # Using count instead of sum since we filtered for TDs

        # Sort and format
        rush_td_agg.sort_values(
            by=['rush_touchdown'], 
            inplace=True, 
            ascending=False
        )

        rush_td_agg.rename(
            columns={
                'player_name': 'Player Name',
                'rush_touchdown': "Total Rushing TD's"
            }, 
            inplace=True
        )

        # Get top 10
        rush_td_agg = rush_td_agg.head(10)

        # Save to CSV
        rush_td_agg.to_csv(rbtds10)
        logger.info(f"RB TDs top 10 for {year} saved to {rbtds10}")
    except Exception as e:
        logger.error(f"Error updating RB TDs top 10 for {year}: {str(e)}")
        update_PBP_data.delay(year)
        update_roster_data.delay(year)

@celery.task()
def update_rec_yds_top10(year=None):
    """Update receiving yards top 10 for specified year"""
    if year is None:
        year = get_selected_year()
        
    recyds10 = os.getcwd() + '/nickknows/nfl/data/' + str(year) + '_rec_yds_top10_data.csv'
    file_path = os.getcwd() + '/nickknows/nfl/data/' + str(year) + '_pbp_data.csv'
    
    if os.path.exists(file_path):
        pbp_data = pd.read_csv(file_path, index_col=0)
    else:
        update_PBP_data.delay(year)
        return f"PBP data for {year} not found, updating in background"
        
    rfile_path = os.getcwd() + '/nickknows/nfl/data/' + str(year) + '_rosters.csv'
    if os.path.exists(rfile_path):
        roster_data = pd.read_csv(rfile_path, index_col=0)
    else:
        update_roster_data.delay(year)
        return f"Roster data for {year} not found, updating in background"
        
    try:
        # Filter for regular season passing plays
        pbp_data = pbp_data[
            (pbp_data["season_type"] == "REG") &
            (pbp_data["two_point_attempt"] == False) &
            (pbp_data["play_type"] == "pass")
        ]

        # Merge with roster data
        pbp_data_rec = pbp_data.merge(
            roster_data[["player_id", "player_name"]], 
            left_on="receiver_player_id", 
            right_on="player_id",
            how='inner'
        )

        # Drop duplicate plays
        pbp_data_rec_unique = pbp_data_rec.drop_duplicates(subset=['game_id', 'play_id'])

        # Group and aggregate receiving yards
        rec_yds_agg = pbp_data_rec_unique.groupby(
            ["player_name"], 
            as_index=False
        ).agg({"receiving_yards": "sum"})

        # Sort and format
        rec_yds_agg.sort_values(
            by=['receiving_yards'], 
            inplace=True, 
            ascending=False
        )

        rec_yds_agg.rename(
            columns={
                'player_name': 'Player Name',
                'receiving_yards': "Total Receiving Yards"
            }, 
            inplace=True
        )

        # Get top 10
        rec_yds_agg = rec_yds_agg.head(10)

        # Save to CSV
        rec_yds_agg.to_csv(recyds10)
        logger.info(f"Receiving yards top 10 for {year} saved to {recyds10}")
    except Exception as e:
        logger.error(f"Error updating receiving yards top 10 for {year}: {str(e)}")
        update_PBP_data.delay(year)
        update_roster_data.delay(year)

@celery.task()
def update_rec_tds_top10(year=None):
    """Update receiving TDs top 10 for specified year"""
    if year is None:
        year = get_selected_year()
        
    rectds10 = os.getcwd() + '/nickknows/nfl/data/' + str(year) + '_rec_tds_top10_data.csv'
    file_path = os.getcwd() + '/nickknows/nfl/data/' + str(year) + '_pbp_data.csv'
    
    if os.path.exists(file_path):
        pbp_data = pd.read_csv(file_path, index_col=0)
    else:
        update_PBP_data.delay(year)
        return f"PBP data for {year} not found, updating in background"
        
    rfile_path = os.getcwd() + '/nickknows/nfl/data/' + str(year) + '_rosters.csv'
    if os.path.exists(rfile_path):
        roster_data = pd.read_csv(rfile_path, index_col=0)
    else:
        update_roster_data.delay(year)
        return f"Roster data for {year} not found, updating in background"
        
    try:
        # Filter for regular season passing touchdown plays
        pbp_data = pbp_data[
            (pbp_data["season_type"] == "REG") &
            (pbp_data["two_point_attempt"] == False) &
            (pbp_data["play_type"] == "pass") &
            (pbp_data["pass_touchdown"] == 1)  # Only count actual touchdowns
        ]

        # Merge with roster data
        pbp_data_rec = pbp_data.merge(
            roster_data[["player_id", "player_name"]], 
            left_on="receiver_player_id", 
            right_on="player_id",
            how='inner'
        )

        # Drop duplicate plays
        pbp_data_rec_unique = pbp_data_rec.drop_duplicates(subset=['game_id', 'play_id'])

        # Group and count unique touchdowns
        rec_td_agg = pbp_data_rec_unique.groupby(
            ["player_name"], 
            as_index=False
        )["pass_touchdown"].count()  # Using count instead of sum since we filtered for TDs

        # Sort and format
        rec_td_agg.sort_values(
            by=['pass_touchdown'], 
            inplace=True, 
            ascending=False
        )

        rec_td_agg.rename(
            columns={
                'player_name': 'Player Name',
                'pass_touchdown': "Total Receiving TD's"
            }, 
            inplace=True
        )

        # Get top 10
        rec_td_agg = rec_td_agg.head(10)

        # Save to CSV
        rec_td_agg.to_csv(rectds10)
        logger.info(f"Receiving TDs top 10 for {year} saved to {rectds10}")
    except Exception as e:
        logger.error(f"Error updating receiving TDs top 10 for {year}: {str(e)}")
        update_PBP_data.delay(year)
        update_roster_data.delay(year)
        
@celery.task()
def update_team_stats(team, year=None):
    """Update team stats for specified team and year"""
    if year is None:
        year = get_selected_year()
        
    # Open Schedule data
    sched_path = os.getcwd() + '/nickknows/nfl/data/' + str(year) + '_schedule.csv'
    schedule = pd.read_csv(sched_path, index_col=0)
    team_dir = os.getcwd() + '/nickknows/nfl/data/' + team + '/'
    if os.path.exists(team_dir):
        pass
    else:
        os.mkdir(os.getcwd() + '/nickknows/nfl/data/' + team + '/')
        
    team_stats = os.getcwd() + '/nickknows/nfl/data/' + team + '/' + str(year) + '_' + team + '_stats.csv'
    team_schedule = os.getcwd() + '/nickknows/nfl/data/' + team + '/' + str(year) + '_' + team + '_schedule.csv'
    
    #Replace game_id with a link that has Away vs. Home instead
    url = (f'<a href="{SITE_DOMAIN}/NFL/PbP/' + 
           schedule['game_id'] + '">' + 
           schedule['away_team'] + ' vs. ' + 
           schedule['home_team'] + '</a>')
    schedule['game_id'] = url
    
    #Create a full schedule for the team selected
    home_team_schedule = schedule.loc[schedule['home_team'] == team]
    away_team_schedule = schedule.loc[schedule['away_team'] == team]
    full_schedule = [home_team_schedule, away_team_schedule]
    full_schedule = pd.concat(full_schedule)
    
    #Drop any games that haven't been played
    full_schedule = full_schedule.dropna(subset=['away_score'])
    full_schedule = full_schedule.sort_values(by=['week'])
    ishome = full_schedule['home_team'].str.contains(team)
    full_schedule['is_home'] = ishome
    op_team1 = full_schedule.loc[full_schedule['is_home'] == True, ['away_team', 'week']]
    op_team2 = full_schedule.loc[full_schedule['is_home'] == False, ['home_team', 'week']]
    op_team = [op_team1, op_team2]
    op_team = pd.concat(op_team)
    op_team["op_team"] = op_team['away_team'].fillna('') + op_team['home_team'].fillna('')
    op_team = op_team.sort_values(by=['week'])
    op_team.to_csv(team_schedule)
    op_team = op_team["op_team"].to_list()
    weekly_team_data = pd.DataFrame()
    
    for opponent in op_team:
        week = op_team.index(opponent) + 1
        rost_path = os.getcwd() + '/nickknows/nfl/data/' + str(year) + '_rosters.csv'
        roster_data = pd.read_csv(rost_path, index_col=0)
        team_roster = roster_data.loc[roster_data['team'] == opponent]
        players = team_roster['player_name'].to_list()
        for player in players:
            week_path = os.getcwd() + '/nickknows/nfl/data/' + str(year) + '_weekly_data.csv'
            weekly_data = pd.read_csv(week_path, index_col=0)
            player_data = weekly_data.loc[weekly_data['player_display_name'] == player]
            player_data = player_data.loc[player_data['week'] == week]
            weekly_team_data = [weekly_team_data, player_data]
            weekly_team_data = pd.concat(weekly_team_data)
    
    weekly_team_data.to_csv(team_stats)
    logger.info(f"Team stats for {team} ({year}) saved to {team_stats}")

@celery.task()
def update_team_schedule(team, year=None):
    """Update team schedule for specified team and year"""
    if year is None:
        year = get_selected_year()
        
    # Open Schedule data
    sched_path = os.getcwd() + '/nickknows/nfl/data/' + str(year) + '_schedule.csv'
    team_sched_path = os.getcwd() + '/nickknows/nfl/data/' + team + '/' + str(year) + '_' + team + '_schedule.csv'
    team_dir = os.getcwd() + '/nickknows/nfl/data/' + team + '/'
    if not os.path.exists(team_dir):
        os.mkdir(team_dir)
        
    schedule = pd.read_csv(sched_path, index_col=0)
    
    # Create full URL with proper formatting
    url = (f'<a href="{SITE_DOMAIN}/NFL/PbP/' + 
           schedule['game_id'] + '">' + 
           schedule['away_team'] + ' vs. ' + 
           schedule['home_team'] + '</a>')
    
    # Ensure URL is not escaped when saved
    schedule['game_id'] = url
    schedule['game_id'] = schedule['game_id'].astype('string')
    
    # Create full schedule for team
    home_team_schedule = schedule.loc[schedule['home_team'] == team]
    away_team_schedule = schedule.loc[schedule['away_team'] == team]
    full_schedule = pd.concat([home_team_schedule, away_team_schedule])
    
    # Drop unplayed games and sort
    full_schedule = full_schedule.dropna(subset=['away_score'])
    full_schedule = full_schedule.sort_values(by=['week'])
    full_schedule.to_csv(team_sched_path)
    
    logger.info(f"Team schedule for {team} ({year}) saved to {team_sched_path}")
    update_weekly_team_data.delay(team, year)

@celery.task()
def update_weekly_team_data(team, year=None):
    """Update weekly team data for specified team and year"""
    if year is None:
        year = get_selected_year()
        
    try:
        # Setup directories
        team_dir = os.getcwd() + '/nickknows/nfl/data/' + team + '/'
        if not os.path.exists(team_dir):
            os.mkdir(team_dir)

        # Read files
        file_path = os.getcwd() + '/nickknows/nfl/data/' + team + '/' + str(year) + '_' + team + '_schedule.csv'
        rost_path = os.getcwd() + '/nickknows/nfl/data/' + str(year) + '_rosters.csv'
        week_path = os.getcwd() + '/nickknows/nfl/data/' + str(year) + '_weekly_data.csv'

        # Load data
        full_schedule = pd.read_csv(file_path, index_col=0)
        roster_data = pd.read_csv(rost_path, index_col=0)
        weekly_data = pd.read_csv(week_path, index_col=0)

        # Identify opponents using home/away logic
        ishome = full_schedule['home_team'].str.contains(team)
        full_schedule['is_home'] = ishome
        op_team1 = full_schedule.loc[full_schedule['is_home'] == True, ['away_team', 'week']]
        op_team2 = full_schedule.loc[full_schedule['is_home'] == False, ['home_team', 'week']]
        opponents = pd.concat([op_team1, op_team2])
        opponents["opponent"] = opponents['away_team'].fillna('') + opponents['home_team'].fillna('')
        opponents = opponents.sort_values(by=['week'])

        # Process weekly data
        weekly_team_data = pd.DataFrame()
        for _, row in opponents.iterrows():
            week = row['week']
            opponent = row['opponent']
            
            logger.info(f"Processing {opponent} week {week} for team {team} ({year})")
            
            # Get opponent roster
            team_roster = roster_data[roster_data['team'] == opponent]
            players = team_roster['player_name'].tolist()
            
            # Get player stats for that week
            player_data = weekly_data[
                (weekly_data['player_display_name'].isin(players)) & 
                (weekly_data['week'] == week) &
                (weekly_data['recent_team'] == opponent)
            ]
            
            weekly_team_data = pd.concat([weekly_team_data, player_data])

        # Save processed data
        output_path = os.getcwd() + '/nickknows/nfl/data/' + team + '/' + str(year) + '_' + team + '_data.csv'
        weekly_team_data.to_csv(output_path)
        
        logger.info(f"Completed weekly team data update for {team} ({year})")
        return True
        
    except Exception as e:
        logger.error(f"Error processing {team} for {year}: {str(e)}")
        raise

def generate_team_graphs(team, weekly_data_dict, year=None):
    """Generate team graphs for specified year"""
    if year is None:
        year = get_selected_year()
        
    weekly_team_data = pd.DataFrame.from_records(weekly_data_dict)
    
    # Process each position's data
    positions = {
        'QB': weekly_team_data[weekly_team_data['position'] == 'QB'],
        'RB': weekly_team_data[weekly_team_data['position'] == 'RB'],
        'WR': weekly_team_data[weekly_team_data['position'] == 'WR'],
        'TE': weekly_team_data[weekly_team_data['position'] == 'TE']
    }
    
    # Calculate standard bar height and figure dimensions
    BAR_HEIGHT = 0.6  # Standard height for each bar
    MIN_HEIGHT = 4    # Minimum figure height in inches
    MAX_HEIGHT = 12   # Maximum figure height in inches
    WIDTH = 10        # Standard figure width in inches
    
    # Generate graphs for each position
    for pos, data in positions.items():
        # Group by player and sum their fantasy points
        player_totals = data
        num_players = len(player_totals)
        
        if num_players == 0:
            continue
        
        # Calculate figure height based on number of players
        fig_height = max(MIN_HEIGHT, min(MAX_HEIGHT, num_players * 0.8))
        
        # Create figure with calculated dimensions
        fig, ax = plt.subplots(figsize=(WIDTH, fig_height))
        
        # Create the horizontal bar plot
        player_totals.plot.barh(x='player_display_name', 
                               y='fantasy_points_ppr', 
                               ylabel='',
                               ax=ax)  # Pass the axis object
        
        # Customize the plot
        plt.title(f'{team} vs {pos}s Fantasy Points ({year})')
        plt.yticks(fontsize=8)
        
        # Calculate appropriate margins based on number of bars
        margin = BAR_HEIGHT / (2 * num_players)
        plt.margins(y=margin)
        
        # Adjust layout
        plt.subplots_adjust(left=0.3)
        
        # Set consistent spacing between bars
        ax.set_ylim(-0.5, num_players - 0.5)
        
        # Adjust bar height using container
        bars = ax.containers[0]
        for bar in bars:
            bar.set_height(BAR_HEIGHT)
        
        # Optional: Add grid lines for better readability
        plt.grid(axis='x', linestyle='--', alpha=0.7)
        
        # Save the plot
        folder_path = '/NickKnows/app/nickknows/static/images/' + team + '/'
        if not os.path.exists(folder_path):
            os.makedirs(folder_path)
            
        plt.savefig(f'/NickKnows/app/nickknows/static/images/{team}/{team}_{pos}_FPA_{year}.png', 
                    bbox_inches='tight',
                    dpi=100)
        plt.close()
        
        logger.info(f"Generated {pos} plot for {team} ({year}) with {len(player_totals)} players")

@celery.task()
def process_team_data(team, year=None):
    """Process team data for specified team and year"""
    if year is None:
        year = get_selected_year()
        
    try:
        data_file_path = os.getcwd() + '/nickknows/nfl/data/' + team + '/' + str(year) + '_' + team + '_data.csv'
        weekly_team_data = pd.read_csv(data_file_path, index_col=0)
        
        # Group by week and position first to get position totals per week
        weekly_position_totals = weekly_team_data.groupby(['week', 'position'])['fantasy_points_ppr'].sum().reset_index()
        
        # Generate plots using existing function
        generate_team_graphs(team, weekly_team_data, year)

        # Calculate mean fantasy points against per position
        pass_agg = weekly_position_totals[weekly_position_totals['position'] == 'QB'].groupby('week')['fantasy_points_ppr'].first().mean()
        rush_agg = weekly_position_totals[weekly_position_totals['position'] == 'RB'].groupby('week')['fantasy_points_ppr'].first().mean()
        rec_agg = weekly_position_totals[weekly_position_totals['position'] == 'WR'].groupby('week')['fantasy_points_ppr'].first().mean()
        te_agg = weekly_position_totals[weekly_position_totals['position'] == 'TE'].groupby('week')['fantasy_points_ppr'].first().mean()
        
        return {
            'Team Name': team,
            'QB': pass_agg,
            'RB': rush_agg,
            'WR': rec_agg,
            'TE': te_agg
        }
    except Exception as e:
        logger.error(f"Error processing team {team} for {year}: {str(e)}")
        raise

@celery.task()
def update_fpa_data(results, year=None):
    """Update FPA data for specified year"""
    if year is None:
        year = get_selected_year()
        
    logger.info(f"Updating FPA data with {len(results)} teams for {year}")
    try:
        fpa_path = os.getcwd() + '/nickknows/nfl/data/' + str(year) + '_FPA.csv'
        df = pd.DataFrame(results)
        df.to_csv(fpa_path)
        logger.info(f"Updated FPA data for {len(results)} teams for {year}")
        return f"Updated FPA data for {len(results)} teams for {year}"
    except Exception as e:
        logger.error(f"Error updating FPA data for {year}: {str(e)}")
        raise

# Wrapper functions to pass year parameter to existing tasks
@celery.task()
def check_data_availability(year=None):
    """Check if data is available for a given year and return status"""
    if year is None:
        year = get_selected_year()
    
    season_display = format_nfl_season(year)
    
    # Test with a simple API call to see if data exists
    try:
        # Try to get just schedule data (lightest call) to test availability
        test_data = nfl.import_schedules([year])
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
                'message': f'Data available for {season_display} ({len(test_data)} games in schedule)'
            }
    except Exception as e:
        return {
            'year': year,
            'season_display': season_display,
            'available': False,
            'message': f'Error checking {season_display}: {str(e)}'
        }

@celery.task()
def update_all_data_for_year(year):
    """Update all NFL data for a specific year with availability checking"""
    season_display = format_nfl_season(year)
    logger.info(f"Starting full data update for {season_display}")
    
    # First check if data is available
    availability = check_data_availability(year)
    if not availability['available']:
        logger.warning(f"Skipping update for {season_display}: {availability['message']}")
        return availability['message']
    
    logger.info(f"Data confirmed available for {season_display}, proceeding with updates")
    
    # Update base data
    update_PBP_data.delay(year)
    update_roster_data.delay(year)
    update_sched_data.delay(year)
    update_week_data.delay(year)
    
    # Wait a bit for base data to process
    time.sleep(30)
    
    # Update aggregated stats
    update_qb_yards_top10.delay(year)
    update_qb_tds_top10.delay(year)
    update_rb_yards_top10.delay(year)
    update_rb_tds_top10.delay(year)
    update_rec_yds_top10.delay(year)
    update_rec_tds_top10.delay(year)
    
    logger.info(f"Completed scheduling all updates for {season_display}")
    return f"Successfully scheduled all updates for {season_display}"

@celery.task()
def update_all_team_fpa_for_year(year):
    """Update FPA data for all teams for a specific year"""
    teams = ['ARI','ATL','BAL','BUF','CAR','CHI','CIN','CLE','DAL','DEN','DET','GB','HOU','IND','JAX','KC','LA','LAC','LV','MIA','MIN','NE','NO','NYG','NYJ','PHI','PIT','SEA','SF','TB','TEN','WAS']
    
    logger.info(f"Starting FPA update for all teams for {year}")
    
    # Create chains for each team that include graph generation
    team_chains = []
    for team in teams:
        team_chains.append(
            chain(
                update_team_schedule.si(team, year),
                update_weekly_team_data.si(team, year),
                process_team_data.si(team, year)
            )
        )
    
    # Execute all chains in parallel and collect results
    chord(team_chains)(update_fpa_data.s(year))
    
    logger.info(f"Completed scheduling FPA updates for all teams for {year}")
    return f"FPA update scheduled for all teams for {year}"