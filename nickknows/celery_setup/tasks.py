from nickknows import celery
import os
import nfl_data_py as nfl
import pandas as pd

@celery.task()
def update_PBP_data():
    file_path = os.getcwd() + '/nickknows/nfl/data/pbp_data.csv'
    pbp_data = nfl.import_pbp_data([2022])
    pbp_data.to_csv(file_path)

@celery.task()
def update_roster_data():
    rfile_path = os.getcwd() + '/nickknows/nfl/data/rosters.csv'
    roster_data = nfl.import_rosters([2022])
    roster_data.to_csv(rfile_path)

@celery.task()
def update_sched_data():
    scfile_path = os.getcwd() + '/nickknows/nfl/data/schedule.csv'
    schedule = nfl.import_schedules([2022])
    schedule.to_csv(scfile_path)

@celery.task()
def update_week_data():
    wefile_path = os.getcwd() + '/nickknows/nfl/data/weekly_data.csv'
    weekly_data = nfl.import_weekly_data([2022])
    weekly_data.to_csv(wefile_path)