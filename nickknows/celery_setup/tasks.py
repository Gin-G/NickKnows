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
        if (time.time() - os.path.getmtime(file_path)) > (7 * 24 * 60 * 60):
            update_PBP_data.delay()
            flash('Data is updating in the background. Refresh the page in a bit')
        else:
            pbp_data = pd.read_csv(file_path, index_col=0)
    else:
        update_PBP_data.delay()
        flash('Data is updating in the background. Refresh the page in a bit')
    rfile_path = os.getcwd() + '/nickknows/nfl/data/' + str(year) + '_rosters.csv'
    if os.path.exists(rfile_path):
        if (time.time() - os.path.getmtime(rfile_path)) > (7 * 24 * 60 * 60):
            update_roster_data.delay()
            flash('Data is updating in the background. Refresh the page in a bit')
        else:
            roster_data = pd.read_csv(rfile_path, index_col=0)
    else:
        update_roster_data.delay()
        flash('Data is updating in the background. Refresh the page in a bit')
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
        if (time.time() - os.path.getmtime(file_path)) > (7 * 24 * 60 * 60):
            update_PBP_data.delay()
            flash('Data is updating in the background. Refresh the page in a bit')
        else:
            pbp_data = pd.read_csv(file_path, index_col=0)
    else:
        update_PBP_data.delay()
        flash('Data is updating in the background. Refresh the page in a bit')
    rfile_path = os.getcwd() + '/nickknows/nfl/data/' + str(year) + '_rosters.csv'
    if os.path.exists(rfile_path):
        if (time.time() - os.path.getmtime(rfile_path)) > (7 * 24 * 60 * 60):
            update_roster_data.delay()
            flash('Data is updating in the background. Refresh the page in a bit')
        else:
            roster_data = pd.read_csv(rfile_path, index_col=0)
    else:
        update_roster_data.delay()
        flash('Data is updating in the background. Refresh the page in a bit')
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
        if (time.time() - os.path.getmtime(file_path)) > (7 * 24 * 60 * 60):
            update_PBP_data.delay()
            flash('Data is updating in the background. Refresh the page in a bit')
        else:
            pbp_data = pd.read_csv(file_path, index_col=0)
    else:
        update_PBP_data.delay()
        flash('Data is updating in the background. Refresh the page in a bit')
    rfile_path = os.getcwd() + '/nickknows/nfl/data/' + str(year) + '_rosters.csv'
    if os.path.exists(rfile_path):
        if (time.time() - os.path.getmtime(rfile_path)) > (7 * 24 * 60 * 60):
            update_roster_data.delay()
            flash('Data is updating in the background. Refresh the page in a bit')
        else:
            roster_data = pd.read_csv(rfile_path, index_col=0)
    else:
        update_roster_data.delay()
        flash('Data is updating in the background. Refresh the page in a bit')
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
        if (time.time() - os.path.getmtime(file_path)) > (7 * 24 * 60 * 60):
            update_PBP_data.delay()
            flash('Data is updating in the background. Refresh the page in a bit')
        else:
            pbp_data = pd.read_csv(file_path, index_col=0)
    else:
        update_PBP_data.delay()
        flash('Data is updating in the background. Refresh the page in a bit')
    rfile_path = os.getcwd() + '/nickknows/nfl/data/' + str(year) + '_rosters.csv'
    if os.path.exists(rfile_path):
        if (time.time() - os.path.getmtime(rfile_path)) > (7 * 24 * 60 * 60):
            update_roster_data.delay()
            flash('Data is updating in the background. Refresh the page in a bit')
        else:
            roster_data = pd.read_csv(rfile_path, index_col=0)
    else:
        update_roster_data.delay()
        flash('Data is updating in the background. Refresh the page in a bit')
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
        if (time.time() - os.path.getmtime(file_path)) > (7 * 24 * 60 * 60):
            update_PBP_data.delay()
            flash('Data is updating in the background. Refresh the page in a bit')
        else:
            pbp_data = pd.read_csv(file_path, index_col=0)
    else:
        update_PBP_data.delay()
        flash('Data is updating in the background. Refresh the page in a bit')
    rfile_path = os.getcwd() + '/nickknows/nfl/data/' + str(year) + '_rosters.csv'
    if os.path.exists(rfile_path):
        if (time.time() - os.path.getmtime(rfile_path)) > (7 * 24 * 60 * 60):
            update_roster_data.delay()
            flash('Data is updating in the background. Refresh the page in a bit')
        else:
            roster_data = pd.read_csv(rfile_path, index_col=0)
    else:
        update_roster_data.delay()
        flash('Data is updating in the background. Refresh the page in a bit')
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
        if (time.time() - os.path.getmtime(file_path)) > (7 * 24 * 60 * 60):
            update_PBP_data.delay()
            flash('Data is updating in the background. Refresh the page in a bit')
        else:
            pbp_data = pd.read_csv(file_path, index_col=0)
    else:
        update_PBP_data.delay()
        flash('Data is updating in the background. Refresh the page in a bit')
    rfile_path = os.getcwd() + '/nickknows/nfl/data/' + str(year) + '_rosters.csv'
    if os.path.exists(rfile_path):
        if (time.time() - os.path.getmtime(rfile_path)) > (7 * 24 * 60 * 60):
            update_roster_data.delay()
            flash('Data is updating in the background. Refresh the page in a bit')
        else:
            roster_data = pd.read_csv(rfile_path, index_col=0)
    else:
        update_roster_data.delay()
        flash('Data is updating in the background. Refresh the page in a bit')
    pbp_data = pbp_data[pbp_data["season_type"] == "REG"]
    pbp_data = pbp_data[pbp_data["two_point_attempt"] == False]
    pbp_data_rec = pbp_data[pbp_data["play_type"] == "pass"]
    pbp_data_rec = pbp_data_rec.merge(roster_data[["player_id","player_name"]], left_on="receiver_player_id", right_on="player_id")
    rec_td_agg = pbp_data_rec.groupby(["player_name"], as_index=False).agg({"pass_touchdown":"sum"})
    rec_td_agg.sort_values(by=['pass_touchdown'], inplace=True, ascending=False)
    rec_td_agg.rename(columns={'player_name':'Player Name',"pass_touchdown":"Total Receiving TD's"}, inplace=True)
    rec_td_agg = rec_td_agg.head(10)
    rec_td_agg.to_csv(rectds10)