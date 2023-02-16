from nickknows import celery
import os
import nfl_data_py as nfl

@celery.task()
def update_data():
    file_path = os.getcwd() + '/nickknows/nfl/data/pbp_data.csv'
    pbp_data = nfl.import_pbp_data([2022])
    print(pbp_data)
    pbp_data.to_csv(file_path)
    rfile_path = os.getcwd() + '/nickknows/nfl/data/rosters.csv'
    roster_data = nfl.import_rosters([2022])
    roster_data.to_csv(rfile_path)
    scfile_path = os.getcwd() + '/nickknows/nfl/data/schedule.csv'
    schedule = nfl.import_schedules([2022])
    schedule.to_csv(scfile_path)
    wefile_path = os.getcwd() + '/nickknows/nfl/data/weekly_data.csv'
    weekly_data = nfl.import_weekly_data([2022])
    weekly_data.to_csv(wefile_path)