from nickknows import celery
import os
import nfl_data_py as nfl
import pandas as pd

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