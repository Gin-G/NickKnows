from glob import escape
from turtle import position
from flask import render_template, url_for, redirect, flash
from numpy import full
from nickknows import app
from ..celery_setup.tasks import update_data
import nfl_data_py as nfl
import pandas as pd
import numpy as np
from IPython.display import HTML
import os
import time
pd.options.mode.chained_assignment = None

@app.route('/NFL')
def NFL():
    return render_template('nfl-home.html')

@app.route('/NFL/update')
def NFLupdate():
    update_data.delay()
    flash('All data is updating in the background. Changes should be reflected on the pages shortly')
    return redirect(url_for('NFL'))
        
@app.route('/NFL/schedule/<week>')
def schedule(week):
    file_path = os.getcwd() + '/nickknows/nfl/data/schedule.csv'
    if os.path.exists(file_path):
        if (time.time() - os.path.getmtime(file_path)) > (7 * 24 * 60 * 60):
            schedule = nfl.import_schedules([2022])
            schedule.to_csv(file_path)
        else:
            schedule = pd.read_csv(file_path, index_col=0)
    else:
        schedule = nfl.import_schedules([2022])
        schedule.to_csv(file_path)
    week_schedule = schedule.loc[schedule['week'] == int(week)]
    url = str('<a href="http://www.nickknows.net/NFL/PbP/') + week_schedule['game_id'] + str('">') + week_schedule['game_id'] + str('</a>')
    week_schedule['game_id'] = url
    week_schedule.rename(columns = {'game_id':'Game ID','gameday':'Date','weekday':'Weekday','gametime':'Game Time','away_team':'Away Team','away_score':'Away Score','home_team':'Home Team','home_score':'Home Score','result':'Result','total':'Total','overtime':'Overtime','away_rest':'Away Rest','home_rest':'Home Rest','away_moneyline':'Away Moneyline','home_moneyline':'Home Moneyline','spread_line':'Spread','away_spread_odds':'Away Spread Odds','home_spread_odds':'Home Spread Odds','total_line':'Total line','under_odds':'Under Odds','over_odds':'Over Odds','div_game':'Division Game','roof':'Roof','surface':'Surface','temp':'Temperature','wind':'Wind','away_qb_name':'Away QB','home_qb_name':'Home QB','away_coach':'Away Coach','home_coach':'Home Coach','referee':'Referee','stadium':'Stadium'}, inplace=True)
    week_schedule = week_schedule.style.hide(axis="index")
    week_schedule = week_schedule.set_table_attributes({'border-collapse' : 'collapse','border-spacing' : '0px'})
    week_schedule = week_schedule.set_table_styles([{'selector': 'th', 'props' : 'background-color : gainsboro; color :black; border: 2px solid black;padding : 2.5px;margin : 0 auto; font-size : 12px'}])
    week_schedule = week_schedule.set_properties(**{'background-color' : 'gainsboro', 'color' :'black', 'border': '2px solid black','padding' : '2.5px','margin' : '0 auto', 'font-size' : '12px'})
    week_schedule = week_schedule.hide(['season','game_type','week','location','old_game_id','gsis','nfl_detail_id','pfr','pff','espn','away_qb_id','home_qb_id','stadium_id'], axis="columns")
    week_schedule = week_schedule.format(precision=2)
    return render_template('weekly.html', week_schedule = HTML(week_schedule.to_html(render_links=True,escape=False)), week = week)

@app.route('/NFL/Roster/<team>/<fullname>')
def roster(team,fullname):
    file_path = os.getcwd() + '/nickknows/nfl/data/rosters.csv'
    if os.path.exists(file_path):
        if (time.time() - os.path.getmtime(file_path)) > (7 * 24 * 60 * 60):
            roster_data = nfl.import_rosters([2022])
            roster_data.to_csv(file_path)
        else:
            roster_data = pd.read_csv(file_path, index_col=0)
    else:
        roster_data = nfl.import_rosters([2022])
        roster_data.to_csv(file_path)
    team_roster = roster_data.loc[roster_data['team'] == team]
    url = str('<a href="https://www.nickknows.net/NFL/Player/') + team_roster['player_name'] + str('">') + team_roster['player_name'] + str('</a>')
    team_roster['player_name'] = url
    team_roster.rename(columns={'depth_chart_position':'Position','jersey_number':'Number','status':'Status','player_name':'Full Name','first_name':'First Name','last_name':'Last Name','height':'Height','weight':'Weight','football_name':'Preferred Name','rookie_year':'Rookie Year','draft_club':'Drafted By','draft_number':'Draft Number'}, inplace=True)
    team_roster = team_roster.style.hide(axis="index")
    team_roster = team_roster.set_table_attributes({'border-collapse' : 'collapse','border-spacing' : '0px'})
    team_roster = team_roster.set_table_styles([{'selector': 'th', 'props' : 'background-color : gainsboro; color:black; border: 2px solid black;padding : 2.5px;margin : 0 auto; font-size : 12px'}])
    team_roster = team_roster.set_properties(**{'background-color' : 'gainsboro', 'color' :'black', 'border': '2px solid black','padding' : '2.5px','margin' : '0 auto', 'font-size' : '12px'})
    team_roster = team_roster.hide(['season','team','position','birth_date','college','player_id','espn_id','sportradar_id','yahoo_id','rotowire_id','pff_id','pfr_id',	'fantasy_data_id','sleeper_id',	'years_exp','headshot_url',	'ngs_position','week','game_type','status_description_abbr','esb_id','gsis_it_id','smart_id','entry_year'], axis="columns")
    team_roster = team_roster.format(precision=0)
    return render_template('rosters.html', team_roster = team_roster.to_html(), team = fullname)

@app.route('/NFL/PbP/<game>')
def game_pbp(game):
    file_path = os.getcwd() + '/nickknows/nfl/data/pbp_data.csv'
    if os.path.exists(file_path):
        if (time.time() - os.path.getmtime(file_path)) > (7 * 24 * 60 * 60):
            pbp_data = nfl.import_pbp_data([2022])
            pbp_data.to_csv(file_path)
        else:
            pbp_data = pd.read_csv(file_path, index_col=0)
    else:
        pbp_data = nfl.import_pbp_data([2022])
        pbp_data.to_csv(file_path)
    game_data = pbp_data.loc[pbp_data['game_id'] == game]
    game_data.rename(columns={'posteam':'Possession','defteam':'Defense','side_of_field':'Field Side','yardline_100':'Distance from EndZone','quarter_seconds_remaining':'Seconds left in Quarter','half_seconds_remaining':'Seconds left in Half','game_seconds_remaining':'Seconds left in Game','drive':'Drive #'}, inplace=True)
    game_data = game_data.style.hide(axis="index")
    game_data = game_data.set_table_attributes({'border-collapse' : 'collapse','border-spacing' : '0px'})
    game_data = game_data.set_table_styles([{'selector': 'th', 'props' : 'background-color : gainsboro; color:black; border: 2px solid black;padding : 2.5px;margin : 0 auto; font-size : 12px'}])
    game_data = game_data.set_properties(**{'background-color' : 'gainsboro', 'color' :'black', 'border': '2px solid black','padding' : '2.5px','margin' : '0 auto', 'font-size' : '12px'})
    game_data = game_data.hide(['play_id','game_id','old_game_id','home_team','away_team','season_type','week','game_date','posteam_type','game_half','quarter_end','sp','qtr','goal_to_go','ydsnet','qb_kneel','qb_spike','qb_scramble'], axis="columns")
    return render_template('pbp.html', game_data = game_data.to_html(), game = game)

@app.route('/NFL/Player/<name>')
def player_stats(name):
    file_path = os.getcwd() + '/nickknows/nfl/data/weekly_data.csv'
    if os.path.exists(file_path):
        if (time.time() - os.path.getmtime(file_path)) > (7 * 24 * 60 * 60):
            weekly_data = nfl.import_weekly_data([2022])
            weekly_data.to_csv(file_path)
        else:
            weekly_data = pd.read_csv(file_path, index_col=0)
    else:
        weekly_data = nfl.import_weekly_data([2022])
        weekly_data.to_csv(file_path)
    player_data = weekly_data.loc[weekly_data['player_display_name'] == name]
    headshot = '<img src="' + player_data['headshot_url'] + '" width="360" >'
    headshot = headshot.unique()
    position = player_data['position'].unique()
    player_data = player_data.style.hide(['player_id','player_name','player_display_name','position','position_group','headshot_url','season','season_type'], axis="columns").hide(axis="index").set_table_attributes({'border-collapse' : 'collapse','border-spacing' : '0px'}).set_table_styles([{'selector': 'th', 'props' : 'background-color : gainsboro; color:black; border: 2px solid black;padding : 2.5px;margin : 0 auto; font-size : 12px'}]).set_properties(**{'background-color' : 'gainsboro', 'color' :'black', 'border': '2px solid black','padding' : '2.5px','margin' : '0 auto', 'font-size' : '12px'})
    return render_template('player-stats.html', player_stats = player_data.to_html(), name = name, headshot = headshot[0], position = position[0])

@app.route('/NFL/Team/<team>/Schedule/<fullname>')
def team_schedule(team, fullname):
    file_path = os.getcwd() + '/nickknows/nfl/data/schedule.csv'
    if os.path.exists(file_path):
        if (time.time() - os.path.getmtime(file_path)) > (7 * 24 * 60 * 60):
            schedule = nfl.import_schedules([2022])
            schedule.to_csv(file_path)
        else:
            schedule = pd.read_csv(file_path, index_col=0)
    else:
        schedule = nfl.import_schedules([2022])
        schedule.to_csv(file_path)
    url = str('<a href="http://www.nickknows.net/NFL/PbP/') + schedule['game_id'] + str('">') + schedule['game_id'] + str('</a>')
    schedule['game_id'] = url
    home_team_schedule = schedule.loc[schedule['home_team'] == team]
    away_team_schedule = schedule.loc[schedule['away_team'] == team]
    full_schedule = [home_team_schedule, away_team_schedule]
    full_schedule = pd.concat(full_schedule)
    full_schedule = full_schedule.sort_values(by=['week'])
    full_schedule = full_schedule.style.hide(axis="index").set_table_attributes({'border-collapse' : 'collapse','border-spacing' : '0px'}).set_table_styles([{'selector': 'th', 'props' : 'background-color : gainsboro; color:black; border: 2px solid black;padding : 2.5px;margin : 0 auto; font-size : 12px'}]).set_properties(**{'background-color' : 'gainsboro', 'color' :'black', 'border': '2px solid black','padding' : '2.5px','margin' : '0 auto', 'font-size' : '12px'}).hide(['season','game_type','location','old_game_id','gsis','nfl_detail_id','pfr','pff','espn','away_qb_id','home_qb_id','stadium_id'], axis="columns")
    return render_template('team-schedule.html', team_schedule = full_schedule.to_html(), fullname = fullname)

@app.route('/NFL/Team/<team>/Results/<fullname>')
def team_results(team, fullname):
    sched_path = os.getcwd() + '/nickknows/nfl/data/schedule.csv'
    if os.path.exists(sched_path):
        if (time.time() - os.path.getmtime(sched_path)) > (7 * 24 * 60 * 60):
            schedule = nfl.import_schedules([2022])
            schedule.to_csv(sched_path)
        else:
            schedule = pd.read_csv(sched_path, index_col=0)
    else:
        schedule = nfl.import_schedules([2022])
        schedule.to_csv(sched_path)
    url = str('<a href="http://www.nickknows.net/NFL/PbP/') + schedule['game_id'] + str('">') + schedule['game_id'] + str('</a>')
    schedule['game_id'] = url
    home_team_schedule = schedule.loc[schedule['home_team'] == team]
    away_team_schedule = schedule.loc[schedule['away_team'] == team]
    full_schedule = [home_team_schedule, away_team_schedule]
    full_schedule = pd.concat(full_schedule)
    full_schedule = full_schedule.dropna(subset=['away_score'])
    full_schedule = full_schedule.sort_values(by=['week'])
    full_schedule = full_schedule.style.hide(axis="index").set_table_attributes({'border-collapse' : 'collapse','border-spacing' : '0px'}).set_table_styles([{'selector': 'th', 'props' : 'background-color : gainsboro; color:black; border: 2px solid black;padding : 2.5px;margin : 0 auto; font-size : 12px'}]).set_properties(**{'background-color' : 'gainsboro', 'color' :'black', 'border': '2px solid black','padding' : '2.5px','margin' : '0 auto', 'font-size' : '12px'}).hide(['season','game_type','location','old_game_id','gsis','nfl_detail_id','pfr','pff','espn','away_qb_id','home_qb_id','stadium_id'], axis="columns")
    return render_template('team-schedule.html', team_schedule = full_schedule.to_html(), fullname = fullname)

@app.route('/NFL/Team/<team>/FPA/<fullname>')
def team_fpa(team, fullname):
    sched_path = os.getcwd() + '/nickknows/nfl/data/schedule.csv'
    if os.path.exists(sched_path):
        if (time.time() - os.path.getmtime(sched_path)) > (7 * 24 * 60 * 60):
            schedule = nfl.import_schedules([2022])
            schedule.to_csv(sched_path)
        else:
            schedule = pd.read_csv(sched_path, index_col=0)
    else:
        schedule = nfl.import_schedules([2022])
        schedule.to_csv(sched_path)
    url = str('<a href="http://www.nickknows.net/NFL/PbP/') + schedule['game_id'] + str('">') + schedule['game_id'] + str('</a>')
    schedule['game_id'] = url
    home_team_schedule = schedule.loc[schedule['home_team'] == team]
    away_team_schedule = schedule.loc[schedule['away_team'] == team]
    full_schedule = [home_team_schedule, away_team_schedule]
    full_schedule = pd.concat(full_schedule)
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
    op_team = op_team["op_team"].to_list()
    weekly_team_data = pd.DataFrame()
    for team in op_team:
        week = op_team.index(team) + 1
        rost_path = os.getcwd() + '/nickknows/nfl/data/rosters.csv'
        if os.path.exists(rost_path):
            if (time.time() - os.path.getmtime(rost_path)) > (7 * 24 * 60 * 60):
                roster_data = nfl.import_rosters([2022])
                roster_data.to_csv(rost_path)
            else:
                roster_data = pd.read_csv(rost_path, index_col=0)
        else:
            roster_data = nfl.import_rosters([2022])
            roster_data.to_csv(rost_path)
        team_roster = roster_data.loc[roster_data['team'] == team]
        players = team_roster['player_name'].to_list()
        for player in players:
            week_path = os.getcwd() + '/nickknows/nfl/data/weekly_data.csv'
            if os.path.exists(week_path):
                if (time.time() - os.path.getmtime(week_path)) > (7 * 24 * 60 * 60):
                    weekly_data = nfl.import_weekly_data([2022])
                    weekly_data.to_csv(week_path)
                else:
                    weekly_data = pd.read_csv(week_path, index_col=0)
            else:
                weekly_data = nfl.import_weekly_data([2022])
                weekly_data.to_csv(week_path)
            player_data = weekly_data.loc[weekly_data['player_display_name'] == player]
            player_data = player_data.loc[player_data['week'] == week]
            weekly_team_data = [weekly_team_data, player_data]
            weekly_team_data = pd.concat(weekly_team_data)
    full_schedule.rename(columns={'game_id':'Play by Play','week':'Week','gameday':'Date','weekday':'Day','gametime':'Time','away_team':'Away Team','away_score':'Away Score','home_team':'Home Team','home_score':'Home Score','result':'Result','total':'Total','overtime':'OT?','away_rest':'Away Rest','home_rest':'Home Rest','away_moneyline':'Away Line Odds','home_moneyline':'Home Line Odds',	'spread_line':'Spread',	'away_spread_odds':'Away Spread Odds',	'home_spread_odds':'Home Spread Odds','total_line':'Total Line','under_odds':'Under Odds','over_odds':'Over Odds','div_game':'Div?','roof':'Roof?','surface':'Field Surface','away_qb_name':'Away QB','home_qb_name':'Home QB',	'away_coach':'Away Coach','home_coach':'Home Coach','referee':'Ref','stadium':'Stadium'}, inplace=True)
    full_schedule = full_schedule.style.hide(axis="index").set_table_attributes({'border-collapse' : 'collapse','border-spacing' : '0px'}).set_table_styles([{'selector': 'th', 'props' : 'background-color : gainsboro; color:black; border: 2px solid black;padding : 2.5px;margin : 0 auto; font-size : 12px'}]).set_properties(**{'background-color' : 'gainsboro', 'color' :'black', 'border': '2px solid black','padding' : '2.5px','margin' : '0 auto', 'font-size' : '12px'}).hide(['season','game_type','location','old_game_id','gsis','nfl_detail_id','pfr','pff','espn','away_qb_id','home_qb_id','stadium_id','temp','wind','is_home'], axis="columns")
    full_schedule = full_schedule.format(precision=1)
    full_schedule = full_schedule.apply(lambda full_schedule: highlight(full_schedule, "Total", "Total Line"), axis=None)
    weeks = weekly_team_data['week'].unique()
    pass_data = weekly_team_data[weekly_team_data['position'] == 'QB']
    pass_agg = pass_data.agg({"fantasy_points_ppr": "sum"})
    pass_data.sort_values(by=['fantasy_points_ppr'], inplace=True, ascending=False)
    rush_data = weekly_team_data[weekly_team_data['position'] == 'RB']
    rush_agg = rush_data.agg({"fantasy_points_ppr": "sum"})
    rush_data.sort_values(by=['fantasy_points_ppr'], inplace=True, ascending=False)
    rec_data = weekly_team_data[weekly_team_data['position'] == 'WR']
    rec_agg = rec_data.agg({"fantasy_points_ppr": "sum"})
    rec_data.sort_values(by=['fantasy_points_ppr'], inplace=True, ascending=False)
    te_data = weekly_team_data[weekly_team_data['position'] == 'TE']
    te_agg = te_data.agg({"fantasy_points_ppr": "sum"})
    te_data.sort_values(by=['fantasy_points_ppr'], inplace=True, ascending=False)
    pass_data = pass_data.drop_duplicates()
    rush_data = rush_data.drop_duplicates()
    rec_data = rec_data.drop_duplicates()
    te_data = te_data.drop_duplicates()
    pass_data = pass_data.style.hide(axis="index").set_table_attributes({'border-collapse' : 'collapse','border-spacing' : '0px'}).set_table_styles([{'selector': 'th', 'props' : 'background-color : gainsboro; color:black; border: 2px solid black;padding : 2.5px;margin : 0 auto; font-size : 12px'}]).set_properties(**{'background-color' : 'gainsboro', 'color' :'black', 'border': '2px solid black','padding' : '2.5px','margin' : '0 auto', 'font-size' : '12px'}).hide(['player_id','player_name','position_group','headshot_url','season','season_type','carries','rushing_yards','rushing_tds','rushing_fumbles','rushing_fumbles_lost','rushing_first_downs','rushing_epa','rushing_2pt_conversions','receptions','targets','receiving_yards','receiving_tds','receiving_fumbles','receiving_fumbles_lost','receiving_air_yards','receiving_yards_after_catch','receiving_first_downs','receiving_epa','receiving_2pt_conversions','racr','target_share','air_yards_share','wopr','special_teams_tds'], axis="columns")
    rush_data = rush_data.style.hide(axis="index").set_table_attributes({'border-collapse' : 'collapse','border-spacing' : '0px'}).set_table_styles([{'selector': 'th', 'props' : 'background-color : gainsboro; color:black; border: 2px solid black;padding : 2.5px;margin : 0 auto; font-size : 12px'}]).set_properties(**{'background-color' : 'gainsboro', 'color' :'black', 'border': '2px solid black','padding' : '2.5px','margin' : '0 auto', 'font-size' : '12px'}).hide(['player_id','player_name','position_group','headshot_url','season','season_type','completions','attempts','passing_yards','passing_tds','interceptions','sacks','sack_yards','sack_fumbles','sack_fumbles_lost','passing_air_yards','passing_yards_after_catch','passing_first_downs','passing_epa','passing_2pt_conversions','pacr','dakota','receiving_epa','racr','air_yards_share','wopr','special_teams_tds'], axis="columns")
    rec_data = rec_data.style.hide(axis="index").set_table_attributes({'border-collapse' : 'collapse','border-spacing' : '0px'}).set_table_styles([{'selector': 'th', 'props' : 'background-color : gainsboro; color:black; border: 2px solid black;padding : 2.5px;margin : 0 auto; font-size : 12px'}]).set_properties(**{'background-color' : 'gainsboro', 'color' :'black', 'border': '2px solid black','padding' : '2.5px','margin' : '0 auto', 'font-size' : '12px'}).hide(['player_id','player_name','position_group','headshot_url','season','season_type','completions','attempts','passing_yards','passing_tds','interceptions','sacks','sack_yards','sack_fumbles','sack_fumbles_lost','passing_air_yards','passing_yards_after_catch','passing_first_downs','passing_epa','passing_2pt_conversions','pacr','dakota','carries','rushing_yards','rushing_tds','rushing_fumbles','rushing_fumbles_lost','rushing_first_downs','rushing_epa','rushing_2pt_conversions'], axis="columns")
    te_data = te_data.style.hide(axis="index").set_table_attributes({'border-collapse' : 'collapse','border-spacing' : '0px'}).set_table_styles([{'selector': 'th', 'props' : 'background-color : gainsboro; color:black; border: 2px solid black;padding : 2.5px;margin : 0 auto; font-size : 12px'}]).set_properties(**{'background-color' : 'gainsboro', 'color' :'black', 'border': '2px solid black','padding' : '2.5px','margin' : '0 auto', 'font-size' : '12px'}).hide(['player_id','player_name','position_group','headshot_url','season','season_type','completions','attempts','passing_yards','passing_tds','interceptions','sacks','sack_yards','sack_fumbles','sack_fumbles_lost','passing_air_yards','passing_yards_after_catch','passing_first_downs','passing_epa','passing_2pt_conversions','pacr','dakota','carries','rushing_yards','rushing_tds','rushing_fumbles','rushing_fumbles_lost','rushing_first_downs','rushing_epa','rushing_2pt_conversions'], axis="columns")
    pass_data = pass_data.format(precision=2)
    rush_data = rush_data.format(precision=2)
    rec_data = rec_data.format(precision=2)
    te_data = te_data.format(precision=2)
    return render_template('team-fpa.html', team_fpa = full_schedule.to_html(), fullname = fullname, rush_data=rush_data.to_html(), pass_data = pass_data.to_html(), rec_data = rec_data.to_html(), te_data = te_data.to_html(), pass_agg = pass_agg['fantasy_points_ppr']/len(weeks), rush_agg = rush_agg['fantasy_points_ppr']/len(weeks), rec_agg = rec_agg['fantasy_points_ppr']/len(weeks), te_agg = te_agg['fantasy_points_ppr']/len(weeks))

def highlight(df, col1, col2):
    mask = df[col1] > df[col2]
    omask = df[col1] < df[col2]
    emask = df[col1] == df[col2]
    new_df = pd.DataFrame("background-color: gainsboro", index=df.index, columns=df.columns)
    new_df[col1] = np.where(mask, "background-color: {}".format("green"), new_df[col1])
    new_df[col1] = np.where(omask, "background-color: {}".format("red"), new_df[col1])
    new_df[col1] = np.where(emask, "background-color: {}".format("yellow"), new_df[col1])
    return new_df
