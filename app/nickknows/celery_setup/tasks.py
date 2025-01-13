from nickknows import celery
import os
import time
import nfl_data_py as nfl
import pandas as pd
import numpy as np
from flask import flash, request
import matplotlib.pyplot as plt
from celery.utils.log import get_task_logger
logger = get_task_logger(__name__)

# Add at top of file with other constants
SITE_DOMAIN = "https://nickknows.ging.nickknows.net"
AVAILABLE_YEARS = list(range(2020, 2025)) 
selected_year = max(AVAILABLE_YEARS)  # Default to the latest year

@celery.task()
def update_PBP_data():
    file_path = os.getcwd() + '/nickknows/nfl/data/' + str(selected_year) + '_pbp_data.csv'
    if selected_year == 2024:
        pbp_data = nfl.import_pbp_data([selected_year], include_participation=False)
    else:
        pbp_data = nfl.import_pbp_data([selected_year])
    pbp_data.to_csv(file_path)

@celery.task()
def update_roster_data():
    rfile_path = os.getcwd() + '/nickknows/nfl/data/' + str(selected_year) + '_rosters.csv'
    roster_data = nfl.import_weekly_rosters([selected_year])
    roster_data.to_csv(rfile_path)

@celery.task()
def update_sched_data():
    scfile_path = os.getcwd() + '/nickknows/nfl/data/' + str(selected_year) + '_schedule.csv'
    schedule = nfl.import_schedules([selected_year])
    schedule.to_csv(scfile_path)

@celery.task()
def update_week_data():
    wefile_path = os.getcwd() + '/nickknows/nfl/data/' + str(selected_year) + '_weekly_data.csv'
    weekly_data = nfl.import_weekly_data([selected_year])
    weekly_data.to_csv(wefile_path)

@celery.task()
def update_qb_yards_top10():
    qb10file_path = os.getcwd() + '/nickknows/nfl/data/' + str(selected_year) + '_qb_yards_top10_data.csv'
    file_path = os.getcwd() + '/nickknows/nfl/data/' + str(selected_year) + '_pbp_data.csv'
    if os.path.exists(file_path):
        pbp_data = pd.read_csv(file_path, index_col=0)
    else:
        update_PBP_data.delay()
        #flash('Data is updating in the background. Refresh the page in a bit')
    rfile_path = os.getcwd() + '/nickknows/nfl/data/' + str(selected_year) + '_rosters.csv'
    if os.path.exists(rfile_path):
        roster_data = pd.read_csv(rfile_path, index_col=0)
    else:
        update_roster_data.delay()
        #flash('Data is updating in the background. Refresh the page in a bit')
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
    except:
        update_PBP_data.delay()
        update_roster_data.delay()

@celery.task()
def update_qb_tds_top10():
    qbtd10file_path = os.getcwd() + '/nickknows/nfl/data/' + str(selected_year) + '_qb_tds_top10_data.csv'
    file_path = os.getcwd() + '/nickknows/nfl/data/' + str(selected_year) + '_pbp_data.csv'
    if os.path.exists(file_path):
            pbp_data = pd.read_csv(file_path, index_col=0)
    else:
        update_PBP_data.delay()
        #flash('Data is updating in the background. Refresh the page in a bit')
    rfile_path = os.getcwd() + '/nickknows/nfl/data/' + str(selected_year) + '_rosters.csv'
    if os.path.exists(rfile_path):
            roster_data = pd.read_csv(rfile_path, index_col=0)
    else:
        update_roster_data.delay()
        #flash('Data is updating in the background. Refresh the page in a bit')
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
    except:
        update_PBP_data.delay()
        update_roster_data.delay()

@celery.task()
def update_rb_yards_top10():
    rbyds10 = os.getcwd() + '/nickknows/nfl/data/' + str(selected_year) + '_rb_yds_top10_data.csv' 
    file_path = os.getcwd() + '/nickknows/nfl/data/' + str(selected_year) + '_pbp_data.csv'
    if os.path.exists(file_path):
        pbp_data = pd.read_csv(file_path, index_col=0)
    else:
        update_PBP_data.delay()
        #flash('Data is updating in the background. Refresh the page in a bit')
    rfile_path = os.getcwd() + '/nickknows/nfl/data/' + str(selected_year) + '_rosters.csv'
    if os.path.exists(rfile_path):
        roster_data = pd.read_csv(rfile_path, index_col=0)
    else:
        update_roster_data.delay()
        #flash('Data is updating in the background. Refresh the page in a bit')
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
    except:
        update_PBP_data.delay()
        update_roster_data.delay()

@celery.task()
def update_rb_tds_top10():
    rbtds10 = os.getcwd() + '/nickknows/nfl/data/' + str(selected_year) + '_rb_tds_top10_data.csv' 
    file_path = os.getcwd() + '/nickknows/nfl/data/' + str(selected_year) + '_pbp_data.csv'
    if os.path.exists(file_path):
        pbp_data = pd.read_csv(file_path, index_col=0)
    else:
        update_PBP_data.delay()
        #flash('Data is updating in the background. Refresh the page in a bit')
    rfile_path = os.getcwd() + '/nickknows/nfl/data/' + str(selected_year) + '_rosters.csv'
    if os.path.exists(rfile_path):
        roster_data = pd.read_csv(rfile_path, index_col=0)
    else:
        update_roster_data.delay()
        #flash('Data is updating in the background. Refresh the page in a bit')
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
    except:
        update_PBP_data.delay()
        update_roster_data.delay()

@celery.task()
def update_rec_yds_top10():
    recyds10 = os.getcwd() + '/nickknows/nfl/data/' + str(selected_year) + '_rec_yds_top10_data.csv' 
    file_path = os.getcwd() + '/nickknows/nfl/data/' + str(selected_year) + '_pbp_data.csv'
    if os.path.exists(file_path):
        pbp_data = pd.read_csv(file_path, index_col=0)
    else:
        update_PBP_data.delay()
        #flash('Data is updating in the background. Refresh the page in a bit')
    rfile_path = os.getcwd() + '/nickknows/nfl/data/' + str(selected_year) + '_rosters.csv'
    if os.path.exists(rfile_path):
        roster_data = pd.read_csv(rfile_path, index_col=0)
    else:
        update_roster_data.delay()
        #flash('Data is updating in the background. Refresh the page in a bit')
    try:
# For receiving yards:
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
    except:
        update_PBP_data.delay()
        update_roster_data.delay()

@celery.task()
def update_rec_tds_top10():
    rectds10 = os.getcwd() + '/nickknows/nfl/data/' + str(selected_year) + '_rec_tds_top10_data.csv' 
    file_path = os.getcwd() + '/nickknows/nfl/data/' + str(selected_year) + '_pbp_data.csv'
    if os.path.exists(file_path):
        pbp_data = pd.read_csv(file_path, index_col=0)
    else:
        update_PBP_data.delay()
        #flash('Data is updating in the background. Refresh the page in a bit')
    rfile_path = os.getcwd() + '/nickknows/nfl/data/' + str(selected_year) + '_rosters.csv'
    if os.path.exists(rfile_path):
        roster_data = pd.read_csv(rfile_path, index_col=0)
    else:
        update_roster_data.delay()
        #flash('Data is updating in the background. Refresh the page in a bit')
    try:
# For receiving touchdowns:
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
    except:
        update_PBP_data.delay()
        update_roster_data.delay()
        
@celery.task()
def update_team_stats(team):
    # Open Schedule data
    sched_path = os.getcwd() + '/nickknows/nfl/data/' + str(selected_year) + '_schedule.csv'
    schedule = pd.read_csv(sched_path, index_col=0)
    team_dir = os.getcwd() + '/nickknows/nfl/data/' + team + '/'
    if os.path.exists(team_dir):
        pass
    else:
        os.mkdir(os.getcwd() + '/nickknows/nfl/data/' + team + '/')
    team_stats = os.getcwd() + '/nickknows/nfl/data/' + team + '/' + str(selected_year) + '_' + team + '_stats.csv' 
    team_schedule = os.getcwd() + '/nickknows/nfl/data/' + team + '/' + str(selected_year) + '_' + team + '_schedule.csv' 
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
    for team in op_team:
        week = op_team.index(team) + 1
        rost_path = os.getcwd() + '/nickknows/nfl/data/' + str(selected_year) + '_rosters.csv'
        roster_data = pd.read_csv(rost_path, index_col=0)
        team_roster = roster_data.loc[roster_data['team'] == team]
        players = team_roster['player_name'].to_list()
        for player in players:
            week_path = os.getcwd() + '/nickknows/nfl/data/' + str(selected_year) + '_weekly_data.csv'
            weekly_data = pd.read_csv(week_path, index_col=0)
            player_data = weekly_data.loc[weekly_data['player_display_name'] == player]
            player_data = player_data.loc[player_data['week'] == week]
            weekly_team_data = [weekly_team_data, player_data]
            weekly_team_data = pd.concat(weekly_team_data)
    weekly_team_data.to_csv(team_stats)

@celery.task()
def update_team_schedule(team):
    # Open Schedule data
    sched_path = os.getcwd() + '/nickknows/nfl/data/' + str(selected_year) + '_schedule.csv'
    team_sched_path = os.getcwd() + '/nickknows/nfl/data/' + team + '/' + str(selected_year) + '_' + team + '_schedule.csv'
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
    update_weekly_team_data.delay(team)

@celery.task()
def update_weekly_team_data(team):
    try:
        # Setup directories
        team_dir = os.getcwd() + '/nickknows/nfl/data/' + team + '/'
        if not os.path.exists(team_dir):
            os.mkdir(team_dir)

        # Read files
        file_path = os.getcwd() + '/nickknows/nfl/data/' + team + '/' + str(selected_year) + '_' + team + '_schedule.csv'
        rost_path = os.getcwd() + '/nickknows/nfl/data/' + str(selected_year) + '_rosters.csv'
        week_path = os.getcwd() + '/nickknows/nfl/data/' + str(selected_year) + '_weekly_data.csv'

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
            
            logger.info(f"Processing {opponent} week {week}")
            
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
        output_path = os.getcwd() + '/nickknows/nfl/data/' + team + '/' + str(selected_year) + '_' + team + '_data.csv'
        weekly_team_data.to_csv(output_path)
        
        logger.info(f"Completed weekly team data update for {team}")
        return True
        
    except Exception as e:
        logger.error(f"Error processing {team}: {str(e)}")
        raise

def generate_team_graphs(team, weekly_data_dict):
    weekly_team_data = pd.DataFrame.from_records(weekly_data_dict)
    
    # Process each position's data
    positions = {
        'QB': weekly_team_data[weekly_team_data['position'] == 'QB'],
        'RB': weekly_team_data[weekly_team_data['position'] == 'RB'],
        'WR': weekly_team_data[weekly_team_data['position'] == 'WR'],
        'TE': weekly_team_data[weekly_team_data['position'] == 'TE']
    }
    
    # Generate graphs for each position
    for pos, data in positions.items():
        # Group by player and sum their fantasy points
        player_totals = data
        
        # Create and save plot
        player_totals.plot.barh(x='player_display_name', y='fantasy_points_ppr', ylabel='')
        plt.title(f'{team} vs {pos}s Fantasy Points')
        plt.tight_layout()
        folder_path = '/NickKnows/app/nickknows/static/images/' + team + '/'
        if not os.path.exists(folder_path):
            os.makedirs(folder_path)
        plt.savefig(f'/NickKnows/app/nickknows/static/images/{team}/{team}_{pos}_FPA.png', bbox_inches='tight')
        plt.close()
        
        logger.info(f"Generated {pos} plot for {team} with {len(player_totals)} players")

@celery.task()
def process_team_data(team):
    try:
        data_file_path = os.getcwd() + '/nickknows/nfl/data/' + team + '/' + str(selected_year) + '_' + team + '_data.csv'
        weekly_team_data = pd.read_csv(data_file_path, index_col=0)
        
        # Group by week and position first to get position totals per week
        weekly_position_totals = weekly_team_data.groupby(['week', 'position'])['fantasy_points_ppr'].sum().reset_index()
        
        # Generate plots using existing function
        generate_team_graphs(team, weekly_team_data)

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
        logger.error(f"Error processing team {team}: {str(e)}")
        raise

@celery.task()
def update_fpa_data(results):
    logger.info(f"Updating FPA data with {len(results)} teams")
    try:
        fpa_path = os.getcwd() + '/nickknows/nfl/data/' + str(selected_year) + '_FPA.csv'
        df = pd.DataFrame(results)
        df.to_csv(fpa_path)
        return f"Updated FPA data for {len(results)} teams"
    except Exception as e:
        logger.error(f"Error updating FPA data: {str(e)}")
        raise