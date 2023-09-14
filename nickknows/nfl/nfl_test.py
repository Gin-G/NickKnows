import nfl_data_py as nfl
import os
import pandas as pd
import numpy as np
import dask.dataframe as dd

'''
Working with the schedule data
'''
'''
schedule = nfl.import_schedules([2022])
team = 'BUF'
home_team_schedule = schedule.loc[schedule['home_team'] == team]
away_team_schedule = schedule.loc[schedule['away_team'] == team]
full_schedule = [home_team_schedule, away_team_schedule]
full_schedule = pd.concat(full_schedule)
full_schedule = full_schedule.dropna(subset=['away_score'])
print(full_schedule.sort_values(by=['week']))

game_id = schedule['game_id']
season = schedule['season']
game_type = schedule['game_type']
week = schedule['week']
gameday = schedule['gameday']
weekday = schedule['weekday']
gametime = schedule['gametime']
away_team = schedule['away_team']
away_score = schedule['away_score']
home_team = schedule['home_team']
home_score = schedule['home_score']
location = schedule['location']
result = schedule['result']
total = schedule['total']
overtime = schedule['overtime']
old_game_id = schedule['old_game_id']
gsis = schedule['gsis']
nfl_detail_id = schedule['nfl_detail_id']
pfr = schedule['pfr']
pff = schedule['pff']
espn = schedule['espn']
away_rest = schedule['away_rest']
home_rest = schedule['home_rest']
away_moneyline = schedule['away_moneyline']
home_moneyline = schedule['home_moneyline']
spread_line = schedule['spread_line']
away_spread_odds = schedule['away_spread_odds']
home_spread_odds = schedule['home_spread_odds']
total_line = schedule['total_line']
under_odds = schedule['under_odds']
over_odds = schedule['over_odds']
div_game = schedule['div_game']
roof = schedule['roof']
surface = schedule['surface']
temp = schedule['temp']
wind = schedule['wind']
away_qb_id = schedule['away_qb_id']
home_qb_id = schedule['home_qb_id']
away_qb_name = schedule['away_qb_name']
home_qb_name = schedule['home_qb_name']
away_coach = schedule['away_coach']
home_coach = schedule['home_coach']
referee = schedule['referee']
stadium_id = schedule['stadium_id']
stadium = schedule['stadium']

#print(schedule.loc[schedule['week'] == 2])
'''
"""
Working with weekly data
"""

weekly = nfl.import_weekly_data([2023],['player_display_name','targets','target_share'])
weekly['target_share'] = weekly['target_share'] * 100
weekly.sort_values(by=['targets'], inplace=True, ascending=False)
print(weekly.head(10))
weekly.sort_values(by=['target_share'], inplace=True, ascending=False)
print(weekly.head(10))

year = 2023

#file_path = os.getcwd() + '/nickknows/nfl/data/' + str(year) + '_pbp_data.csv'
#pbp_data = nfl.import_pbp_data([year])
#pbp_data.to_csv(file_path)

'''
### All weekly columns
player_name
player_display_name
position
position_group
headshot_url
recent_team
season
week
season_type
completions
attempts
passing_yards
passing_tds
interceptions
sacks
sack_yards
sack_fumbles
sack_fumbles_lost
passing_air_yards
passing_yards_after_catch
passing_first_downs
passing_epa
passing_2pt_conversions
pacr
dakota
carries
rushing_yards
rushing_tds
rushing_fumbles
rushing_fumbles_lost
rushing_first_downs
rushing_epa
rushing_2pt_conversions
receptions
targets
receiving_yards
receiving_tds
receiving_fumbles
receiving_fumbles_lost
receiving_air_yards
receiving_yards_after_catch
receiving_first_downs
receiving_epa
receiving_2pt_conversions
racr
target_share
air_yards_share
wopr
special_teams_tds
fantasy_points
fantasy_points_ppr
'''

#print(weekly.loc[weekly['player_name'] == 'C.Akers'])


'''
Working with snap counts
'''
'''
snap_counts = nfl.import_snap_counts([2022])

game_id = snap_counts['game_id']
pfr_game_id = snap_counts['pfr_game_id']
season = snap_counts['season']
game_type = snap_counts['game_type']
week = snap_counts['week']
player = snap_counts['player']
pfr_player_id = snap_counts['pfr_player_id']
position = snap_counts['position']
team = snap_counts['team']
opponent = snap_counts['opponent']
offense_snaps = snap_counts['offense_snaps']
offense_pct = snap_counts['offense_pct']
defense_snaps = snap_counts['defense_snaps']
defense_pct = snap_counts['defense_pct']
st_snaps = snap_counts['st_snaps']
st_pct = snap_counts['st_pct']

#print(snap_counts.loc[snap_counts['week'] == 2])
'''
'''
Working with play by play data
'''

#pbp_data = nfl.import_pbp_data([2022])
#print(pbp_data.loc[pbp_data['game_id'] == '2022_02_ARI_LV'])

'''
Working with roster data
'''
#roster_data = nfl.import_rosters([2022])
#print(roster_data.loc[roster_data['team'] == 'ARI'])
#team_list = []
#teams = roster_data['team'].tolist()
#for team in teams:
#    if team not in team_list:
#        team_list.append(team)


"""
Working with nflgame _ NO It's for Python 2...
"""

