from nickknows import celery
import os
import time
import nfl_data_py as nfl
import pandas as pd
from flask import flash

year = 2023

@celery.task()
def update_PBP_data():
    file_path = os.getcwd() + '/nickknows/nfl/data/' + str(year) + '_pbp_data.csv'
    pbp_data = nfl.import_pbp_data([year])
    pbp_data.to_csv(file_path)

@celery.task()
def update_roster_data():
    rfile_path = os.getcwd() + '/nickknows/nfl/data/' + str(year) + '_rosters.csv'
    roster_data = nfl.import_rosters([year])
    roster_data.to_csv(rfile_path)

@celery.task()
def update_sched_data():
    scfile_path = os.getcwd() + '/nickknows/nfl/data/' + str(year) + '_schedule.csv'
    schedule = nfl.import_schedules([year])
    schedule.to_csv(scfile_path)

@celery.task()
def update_week_data():
    wefile_path = os.getcwd() + '/nickknows/nfl/data/' + str(year) + '_weekly_data.csv'
    weekly_data = nfl.import_weekly_data([year])
    weekly_data.to_csv(wefile_path)

@celery.task()
def update_qb_yards_top10():
    qb10file_path = os.getcwd() + '/nickknows/nfl/data/' + str(year) + '_qb_yards_top10_data.csv'
    file_path = os.getcwd() + '/nickknows/nfl/data/' + str(year) + '_pbp_data.csv'
    if os.path.exists(file_path):
        pbp_data = pd.read_csv(file_path, index_col=0)
    else:
        update_PBP_data.delay()
        #flash('Data is updating in the background. Refresh the page in a bit')
    rfile_path = os.getcwd() + '/nickknows/nfl/data/' + str(year) + '_rosters.csv'
    if os.path.exists(rfile_path):
        roster_data = pd.read_csv(rfile_path, index_col=0)
    else:
        update_roster_data.delay()
        #flash('Data is updating in the background. Refresh the page in a bit')
    pbp_data = pbp_data[pbp_data["season_type"] == "REG"]
    pbp_data = pbp_data[pbp_data["two_point_attempt"] == False]
    pbp_data_pass = pbp_data[pbp_data["play_type"] == "pass"]
    pbp_data_pass = pbp_data_pass.merge(roster_data[["player_id","player_name"]], left_on="passer_player_id", right_on="player_id")
    pass_agg = pbp_data_pass.groupby(["player_name"], as_index=False).agg({"passing_yards": "sum"})
    pass_agg.sort_values(by=['passing_yards'], inplace=True, ascending=False)
    pass_agg.rename(columns={'player_name':'Player Name','passing_yards':"Total Passing Yards"}, inplace=True)
    pass_agg = pass_agg.head(10)
    pass_agg.to_csv(qb10file_path)

@celery.task()
def update_qb_tds_top10():
    qbtd10file_path = os.getcwd() + '/nickknows/nfl/data/' + str(year) + '_qb_tds_top10_data.csv'
    file_path = os.getcwd() + '/nickknows/nfl/data/' + str(year) + '_pbp_data.csv'
    if os.path.exists(file_path):
            pbp_data = pd.read_csv(file_path, index_col=0)
    else:
        update_PBP_data.delay()
        #flash('Data is updating in the background. Refresh the page in a bit')
    rfile_path = os.getcwd() + '/nickknows/nfl/data/' + str(year) + '_rosters.csv'
    if os.path.exists(rfile_path):
            roster_data = pd.read_csv(rfile_path, index_col=0)
    else:
        update_roster_data.delay()
        #flash('Data is updating in the background. Refresh the page in a bit')
    pbp_data = pbp_data[pbp_data["season_type"] == "REG"]
    pbp_data = pbp_data[pbp_data["two_point_attempt"] == False]
    pbp_data_pass = pbp_data[pbp_data["play_type"] == "pass"]
    pbp_data_pass = pbp_data_pass.merge(roster_data[["player_id","player_name"]], left_on="passer_player_id", right_on="player_id")
    pass_td_agg = pbp_data_pass.groupby(["player_name"], as_index=False).agg({"pass_touchdown":"sum"})
    pass_td_agg.sort_values(by=['pass_touchdown'], inplace=True, ascending=False)
    pass_td_agg.rename(columns={'player_name':'Player Name',"pass_touchdown":"Total Passing TD's"}, inplace=True)
    pass_td_agg = pass_td_agg.head(10)
    pass_td_agg.to_csv(qbtd10file_path)

@celery.task()
def update_rb_yards_top10():
    rbyds10 = os.getcwd() + '/nickknows/nfl/data/' + str(year) + '_rb_yds_top10_data.csv' 
    file_path = os.getcwd() + '/nickknows/nfl/data/' + str(year) + '_pbp_data.csv'
    if os.path.exists(file_path):
        pbp_data = pd.read_csv(file_path, index_col=0)
    else:
        update_PBP_data.delay()
        #flash('Data is updating in the background. Refresh the page in a bit')
    rfile_path = os.getcwd() + '/nickknows/nfl/data/' + str(year) + '_rosters.csv'
    if os.path.exists(rfile_path):
        roster_data = pd.read_csv(rfile_path, index_col=0)
    else:
        update_roster_data.delay()
        #flash('Data is updating in the background. Refresh the page in a bit')
    pbp_data = pbp_data[pbp_data["season_type"] == "REG"]
    pbp_data = pbp_data[pbp_data["two_point_attempt"] == False]
    pbp_data_rush = pbp_data[pbp_data["play_type"] == "run"]
    pbp_data_rush = pbp_data_rush.merge(roster_data[["player_id","player_name"]], left_on="rusher_player_id", right_on="player_id")
    rush_yds_agg = pbp_data_rush.groupby(["player_name"], as_index=False).agg({"rushing_yards": "sum"})
    rush_yds_agg.sort_values(by=['rushing_yards'], inplace=True, ascending=False)
    rush_yds_agg.rename(columns={'player_name':'Player Name','rushing_yards':"Total Rushing Yards"}, inplace=True)
    rush_yds_agg = rush_yds_agg.head(10)
    rush_yds_agg.to_csv(rbyds10)

@celery.task()
def update_rb_tds_top10():
    rbtds10 = os.getcwd() + '/nickknows/nfl/data/' + str(year) + '_rb_tds_top10_data.csv' 
    file_path = os.getcwd() + '/nickknows/nfl/data/' + str(year) + '_pbp_data.csv'
    if os.path.exists(file_path):
        pbp_data = pd.read_csv(file_path, index_col=0)
    else:
        update_PBP_data.delay()
        #flash('Data is updating in the background. Refresh the page in a bit')
    rfile_path = os.getcwd() + '/nickknows/nfl/data/' + str(year) + '_rosters.csv'
    if os.path.exists(rfile_path):
        roster_data = pd.read_csv(rfile_path, index_col=0)
    else:
        update_roster_data.delay()
        #flash('Data is updating in the background. Refresh the page in a bit')
    pbp_data = pbp_data[pbp_data["season_type"] == "REG"]
    pbp_data = pbp_data[pbp_data["two_point_attempt"] == False]
    pbp_data_rush = pbp_data[pbp_data["play_type"] == "run"]
    pbp_data_rush = pbp_data_rush.merge(roster_data[["player_id","player_name"]], left_on="rusher_player_id", right_on="player_id")
    rush_td_agg = pbp_data_rush.groupby(["player_name"], as_index=False).agg({"rush_touchdown":"sum"})
    rush_td_agg.sort_values(by=['rush_touchdown'], inplace=True, ascending=False)
    rush_td_agg.rename(columns={'player_name':'Player Name',"rush_touchdown":"Total Rushing TD's"}, inplace=True)
    rush_td_agg = rush_td_agg.head(10)
    rush_td_agg.to_csv(rbtds10)

@celery.task()
def update_rec_yds_top10():
    recyds10 = os.getcwd() + '/nickknows/nfl/data/' + str(year) + '_rec_yds_top10_data.csv' 
    file_path = os.getcwd() + '/nickknows/nfl/data/' + str(year) + '_pbp_data.csv'
    if os.path.exists(file_path):
        pbp_data = pd.read_csv(file_path, index_col=0)
    else:
        update_PBP_data.delay()
        #flash('Data is updating in the background. Refresh the page in a bit')
    rfile_path = os.getcwd() + '/nickknows/nfl/data/' + str(year) + '_rosters.csv'
    if os.path.exists(rfile_path):
        roster_data = pd.read_csv(rfile_path, index_col=0)
    else:
        update_roster_data.delay()
        #flash('Data is updating in the background. Refresh the page in a bit')
    pbp_data = pbp_data[pbp_data["season_type"] == "REG"]
    pbp_data = pbp_data[pbp_data["two_point_attempt"] == False]
    pbp_data_rec = pbp_data[pbp_data["play_type"] == "pass"]
    pbp_data_rec = pbp_data_rec.merge(roster_data[["player_id","player_name"]], left_on="receiver_player_id", right_on="player_id")
    rec_yds_agg = pbp_data_rec.groupby(["player_name"], as_index=False).agg({"receiving_yards": "sum"})
    rec_yds_agg.sort_values(by=['receiving_yards'], inplace=True, ascending=False)
    rec_yds_agg.rename(columns={'player_name':'Player Name','receiving_yards':"Total Receiving Yards"}, inplace=True)
    rec_yds_agg = rec_yds_agg.head(10)
    rec_yds_agg.to_csv(recyds10)

@celery.task()
def update_rec_tds_top10():
    rectds10 = os.getcwd() + '/nickknows/nfl/data/' + str(year) + '_rec_tds_top10_data.csv' 
    file_path = os.getcwd() + '/nickknows/nfl/data/' + str(year) + '_pbp_data.csv'
    if os.path.exists(file_path):
        pbp_data = pd.read_csv(file_path, index_col=0)
    else:
        update_PBP_data.delay()
        #flash('Data is updating in the background. Refresh the page in a bit')
    rfile_path = os.getcwd() + '/nickknows/nfl/data/' + str(year) + '_rosters.csv'
    if os.path.exists(rfile_path):
        roster_data = pd.read_csv(rfile_path, index_col=0)
    else:
        update_roster_data.delay()
        #flash('Data is updating in the background. Refresh the page in a bit')
    pbp_data = pbp_data[pbp_data["season_type"] == "REG"]
    pbp_data = pbp_data[pbp_data["two_point_attempt"] == False]
    pbp_data_rec = pbp_data[pbp_data["play_type"] == "pass"]
    pbp_data_rec = pbp_data_rec.merge(roster_data[["player_id","player_name"]], left_on="receiver_player_id", right_on="player_id")
    rec_td_agg = pbp_data_rec.groupby(["player_name"], as_index=False).agg({"pass_touchdown":"sum"})
    rec_td_agg.sort_values(by=['pass_touchdown'], inplace=True, ascending=False)
    rec_td_agg.rename(columns={'player_name':'Player Name',"pass_touchdown":"Total Receiving TD's"}, inplace=True)
    rec_td_agg = rec_td_agg.head(10)
    rec_td_agg.to_csv(rectds10)

@celery.task()
def update_team_stats(team):
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
    url = str('<a href="http://localhost:5000/NFL/PbP/') + schedule['game_id'] + str('">') + schedule['away_team'] + ' vs. ' + schedule['home_team'] + str('</a>')
    #url = str('<a href="https://www.nickknows.net/NFL/PbP/') + schedule['game_id'] + str('">') + schedule['away_team'] + ' vs. ' + schedule['home_team'] + str('</a>')
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
        rost_path = os.getcwd() + '/nickknows/nfl/data/' + str(year) + '_rosters.csv'
        roster_data = pd.read_csv(rost_path, index_col=0)
        team_roster = roster_data.loc[roster_data['team'] == team]
        players = team_roster['player_name'].to_list()
        for player in players:
            week_path = os.getcwd() + '/nickknows/nfl/data/' + str(year) + '_weekly_data.csv'
            weekly_data = pd.read_csv(week_path, index_col=0)
            player_data = weekly_data.loc[weekly_data['player_display_name'] == player]
            player_data = player_data.loc[player_data['week'] == week]
            weekly_team_data = [weekly_team_data, player_data]
            weekly_team_data = pd.concat(weekly_team_data)
    weekly_team_data.to_csv(team_stats)

@celery.task()
def update_team_schedule(team):
    # Open Schedule data
    sched_path = os.getcwd() + '/nickknows/nfl/data/' + str(year) + '_schedule.csv'
    team_sched_path = os.getcwd() + '/nickknows/nfl/data/' + team + '/' + str(year) + '_' + team + '_schedule.csv'
    team_dir = os.getcwd() + '/nickknows/nfl/data/' + team + '/'
    if os.path.exists(team_dir):
        pass
    else:
        os.mkdir(os.getcwd() + '/nickknows/nfl/data/' + team + '/')
    schedule = pd.read_csv(sched_path, index_col=0)
    #Replace game_id with a link that has Away vs. Home instead
    url = str('<a href="http://localhost:5000/NFL/PbP/') + schedule['game_id'] + str('">') + schedule['away_team'] + ' vs. ' + schedule['home_team'] + str('</a>')
    #url = str('<a href="https://www.nickknows.net/NFL/PbP/') + schedule['game_id'] + str('">') + schedule['away_team'] + ' vs. ' + schedule['home_team'] + str('</a>')
    schedule['game_id'] = url
    #Create a full schedule for the team selected
    home_team_schedule = schedule.loc[schedule['home_team'] == team]
    away_team_schedule = schedule.loc[schedule['away_team'] == team]
    full_schedule = [home_team_schedule, away_team_schedule]
    full_schedule = pd.concat(full_schedule)
    #Drop any games that haven't been played
    full_schedule = full_schedule.dropna(subset=['away_score'])
    full_schedule = full_schedule.sort_values(by=['week'])
    full_schedule.to_csv(team_sched_path)
    update_weekly_team_data.delay(team)
    
@celery.task()
def update_weekly_team_data(team):
    file_path = os.getcwd() + '/nickknows/nfl/data/' + team + '/' + str(year) + '_' + team + '_schedule.csv'
    data_file_path = os.getcwd() + '/nickknows/nfl/data/' + team + '/' + str(year) + '_' + team + '_data.csv'
    team_dir = os.getcwd() + '/nickknows/nfl/data/' + team + '/'
    if os.path.exists(team_dir):
        pass
    else:
        os.mkdir(os.getcwd() + '/nickknows/nfl/data/' + team + '/')
    full_schedule = pd.read_csv(file_path, index_col=0)
    ishome = full_schedule['home_team'].str.contains(team)
    full_schedule['is_home'] = ishome
    op_team1 = full_schedule.loc[full_schedule['is_home'] == True, ['away_team', 'week']]
    op_team2 = full_schedule.loc[full_schedule['is_home'] == False, ['home_team', 'week']]
    op_team = [op_team1, op_team2]
    op_team = pd.concat(op_team)
    op_team["op_team"] = op_team['away_team'].fillna('') + op_team['home_team'].fillna('')
    op_team = op_team.sort_values(by=['week'])
    op_team = op_team["op_team"].to_list()
    weekly_team_data = pd.DataFrame()
    for team in op_team:
        week = op_team.index(team) + 1
        rost_path = os.getcwd() + '/nickknows/nfl/data/' + str(year) + '_rosters.csv'
        roster_data = pd.read_csv(rost_path, index_col=0)
        team_roster = roster_data.loc[roster_data['team'] == team]
        players = team_roster['player_name'].to_list()
        for player in players:
            week_path = os.getcwd() + '/nickknows/nfl/data/' + str(year) + '_weekly_data.csv'
            weekly_data = pd.read_csv(week_path, index_col=0)
            player_data = weekly_data.loc[weekly_data['player_display_name'] == player]
            player_data = player_data.loc[player_data['week'] == week]
            weekly_team_data = [weekly_team_data, player_data]
            weekly_team_data = pd.concat(weekly_team_data)
    weekly_team_data.to_csv(data_file_path)