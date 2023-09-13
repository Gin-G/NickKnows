from glob import escape
from turtle import position
from flask import render_template, url_for, redirect, flash
from numpy import full
from nickknows import app
from ..celery_setup.tasks import update_PBP_data, update_roster_data, update_sched_data, update_week_data, update_qb_yards_top10, update_qb_tds_top10, update_rb_yards_top10, update_rb_tds_top10, update_rec_yds_top10, update_rec_tds_top10
import nfl_data_py as nfl
import pandas as pd
import numpy as np
import dask.dataframe as dd
from IPython.display import HTML
import os
import time
pd.options.mode.chained_assignment = None

year = 2023

@app.route('/NFL')
def NFL():
    try:
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
        qb10file_path = os.getcwd() + '/nickknows/nfl/data/' + str(year) + '_qb_yards_top10_data.csv'
        qbtd10file_path = os.getcwd() + '/nickknows/nfl/data/' + str(year) + '_qb_tds_top10_data.csv'
        rbyds10 = os.getcwd() + '/nickknows/nfl/data/' + str(year) + '_rb_yds_top10_data.csv' 
        rbtds10 = os.getcwd() + '/nickknows/nfl/data/' + str(year) + '_rb_tds_top10_data.csv' 
        recyds10 = os.getcwd() + '/nickknows/nfl/data/' + str(year) + '_rec_yds_top10_data.csv' 
        rectds10 = os.getcwd() + '/nickknows/nfl/data/' + str(year) + '_rec_tds_top10_data.csv' 
        pass_agg = pd.read_csv(qb10file_path)
        pass_td_agg = pd.read_csv(qbtd10file_path)
        rush_yds_agg = pd.read_csv(rbyds10)
        rush_td_agg = pd.read_csv(rbtds10)
        rec_yds_agg = pd.read_csv(recyds10)
        rec_td_agg = pd.read_csv(rectds10)
        pass_agg.drop(columns=pass_agg.columns[0], axis=1,  inplace=True)
        pass_td_agg.drop(columns=pass_td_agg.columns[0], axis=1,  inplace=True)
        rush_yds_agg.drop(columns=rush_yds_agg.columns[0], axis=1,  inplace=True)
        rush_td_agg.drop(columns=rush_td_agg.columns[0], axis=1,  inplace=True)
        rec_yds_agg.drop(columns=rec_yds_agg.columns[0], axis=1,  inplace=True)
        rec_td_agg.drop(columns=rec_td_agg.columns[0], axis=1,  inplace=True)
        pass_agg = pass_agg.style.hide(axis="index").set_table_attributes({'border-collapse' : 'collapse','border-spacing' : '0px'}).set_table_styles([{'selector': 'th', 'props' : 'background-color : gainsboro; color:black; border: 2px solid black;padding : 2.5px;margin : 0 auto; font-size : 12px'}]).set_properties(**{'background-color' : 'gainsboro', 'color' :'black', 'border': '2px solid black','padding' : '2.5px','margin' : '0 auto', 'font-size' : '12px'})
        pass_td_agg = pass_td_agg.style.hide(axis="index").set_table_attributes({'border-collapse' : 'collapse','border-spacing' : '0px'}).set_table_styles([{'selector': 'th', 'props' : 'background-color : gainsboro; color:black; border: 2px solid black;padding : 2.5px;margin : 0 auto; font-size : 12px'}]).set_properties(**{'background-color' : 'gainsboro', 'color' :'black', 'border': '2px solid black','padding' : '2.5px','margin' : '0 auto', 'font-size' : '12px'})
        rush_yds_agg = rush_yds_agg.style.hide(axis="index").set_table_attributes({'border-collapse' : 'collapse','border-spacing' : '0px'}).set_table_styles([{'selector': 'th', 'props' : 'background-color : gainsboro; color:black; border: 2px solid black;padding : 2.5px;margin : 0 auto; font-size : 12px'}]).set_properties(**{'background-color' : 'gainsboro', 'color' :'black', 'border': '2px solid black','padding' : '2.5px','margin' : '0 auto', 'font-size' : '12px'})
        rush_td_agg = rush_td_agg.style.hide(axis="index").set_table_attributes({'border-collapse' : 'collapse','border-spacing' : '0px'}).set_table_styles([{'selector': 'th', 'props' : 'background-color : gainsboro; color:black; border: 2px solid black;padding : 2.5px;margin : 0 auto; font-size : 12px'}]).set_properties(**{'background-color' : 'gainsboro', 'color' :'black', 'border': '2px solid black','padding' : '2.5px','margin' : '0 auto', 'font-size' : '12px'})
        rec_yds_agg = rec_yds_agg.style.hide(axis="index").set_table_attributes({'border-collapse' : 'collapse','border-spacing' : '0px'}).set_table_styles([{'selector': 'th', 'props' : 'background-color : gainsboro; color:black; border: 2px solid black;padding : 2.5px;margin : 0 auto; font-size : 12px'}]).set_properties(**{'background-color' : 'gainsboro', 'color' :'black', 'border': '2px solid black','padding' : '2.5px','margin' : '0 auto', 'font-size' : '12px'})
        rec_td_agg = rec_td_agg.style.hide(axis="index").set_table_attributes({'border-collapse' : 'collapse','border-spacing' : '0px'}).set_table_styles([{'selector': 'th', 'props' : 'background-color : gainsboro; color:black; border: 2px solid black;padding : 2.5px;margin : 0 auto; font-size : 12px'}]).set_properties(**{'background-color' : 'gainsboro', 'color' :'black', 'border': '2px solid black','padding' : '2.5px','margin' : '0 auto', 'font-size' : '12px'})
        pass_agg = pass_agg.format(precision=0)
        pass_td_agg = pass_td_agg.format(precision=0)
        rush_yds_agg = rush_yds_agg.format(precision=0)
        rush_td_agg = rush_td_agg.format(precision=0)
        rec_yds_agg = rec_yds_agg.format(precision=0)
        rec_td_agg = rec_td_agg.format(precision=0)
        return render_template('nfl-home.html', pass_yards_data = pass_agg.to_html(), pass_td_data = pass_td_agg.to_html(), rush_yards_data = rush_yds_agg.to_html(), rush_td_data = rush_td_agg.to_html(), rec_yards_data = rec_yds_agg.to_html(), rec_td_data = rec_td_agg.to_html())
    except Exception as e:
        flash(e)
        return render_template('nfl-home.html')

@app.route('/NFL/update')
def NFLupdate():
    update_PBP_data.delay()
    update_roster_data.delay()
    update_sched_data.delay()
    update_week_data.delay()
    update_qb_yards_top10.delay()
    update_qb_tds_top10.delay()
    update_rb_yards_top10.delay()
    update_rb_tds_top10.delay()
    update_rec_yds_top10.delay()
    update_rec_tds_top10.delay()
    flash('All data is updating in the background. Changes should be reflected on the pages shortly')
    return redirect(url_for('NFL'))
        
@app.route('/NFL/schedule/<week>')
def schedule(week):
    file_path = os.getcwd() + '/nickknows/nfl/data/' + str(year) + '_schedule.csv'
    schedule = pd.read_csv(file_path, index_col=0)
    week_schedule = schedule.loc[schedule['week'] == int(week)]
    url = str('<a href="http://localhost:5000/NFL/PbP/') + week_schedule['game_id'] + str('">') + week_schedule['away_team'] + ' vs. ' + week_schedule['home_team'] + str('</a>')
    week_schedule['game_id'] = url
    week_schedule.rename(columns = {'game_id':'Game','away_team':'Away Team','away_score':'Away Score','home_team':'Home Team','home_score':'Home Score','result':'Result','total':'Total','overtime':'Overtime','away_rest':'Away Rest','home_rest':'Home Rest','away_moneyline':'Away Moneyline','home_moneyline':'Home Moneyline','spread_line':'Spread','away_spread_odds':'Away Spread Odds','home_spread_odds':'Home Spread Odds','total_line':'Total Line','under_odds':'Under Odds','over_odds':'Over Odds','div_game':'Division Game','roof':'Roof','away_qb_name':'Away QB','home_qb_name':'Home QB','stadium':'Stadium'}, inplace=True)
    week_schedule = week_schedule.style.hide(axis="index")
    week_schedule = week_schedule.set_table_attributes({'border-collapse' : 'collapse','border-spacing' : '0px'})
    week_schedule = week_schedule.set_table_styles([{'selector': 'th', 'props' : 'background-color : gainsboro; color :black; border: 2px solid black;padding : 2.5px;margin : 0 auto; font-size : 12px'}])
    week_schedule = week_schedule.set_properties(**{'background-color' : 'gainsboro', 'color' :'black', 'border': '2px solid black','padding' : '2.5px','margin' : '0 auto', 'font-size' : '12px'})
    week_schedule = week_schedule.hide(['gameday','weekday','gametime','season','game_type','ftn','week','location','old_game_id','gsis','nfl_detail_id','surface','temp','wind','pfr','pff','espn','away_qb_id','home_qb_id','away_coach','home_coach','referee','stadium_id'], axis="columns")
    week_schedule = week_schedule.format(precision=2)
    week_schedule.apply(lambda week_schedule: highlight(week_schedule, "Result", "Spread"), axis=None).apply(lambda week_schedule: highlight(week_schedule, "Total", "Total Line"), axis=None)
    #week_schedule.apply(lambda week_schedule: highlight(week_schedule, "Result", "Spread"), axis=None)
    return render_template('weekly.html', week_schedule = HTML(week_schedule.to_html(render_links=True,escape=False)), week = week)

@app.route('/NFL/Roster/<team>/<fullname>')
def roster(team,fullname):
    file_path = os.getcwd() + '/nickknows/nfl/data/' + str(year) + '_rosters.csv'
    roster_data = pd.read_csv(file_path, index_col=0)
    team_roster = roster_data.loc[roster_data['team'] == team]
    url = str('<a href="http://localhost:5000/NFL/Player/') + team_roster['player_name'] + str('">') + team_roster['player_name'] + str('</a>')
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
    file_path = os.getcwd() + '/nickknows/nfl/data/' + str(year) + '_pbp_data.csv'
    pbp_data = pd.read_csv(file_path, index_col=0)
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
    file_path = os.getcwd() + '/nickknows/nfl/data/' + str(year) + '_weekly_data.csv'
    weekly_data = pd.read_csv(file_path, index_col=0)
    player_data = weekly_data.loc[weekly_data['player_display_name'] == name]
    headshot = '<img src="' + player_data['headshot_url'] + '" width="360" >'
    headshot = headshot.unique()
    position = player_data['position'].unique()
    player_data = player_data.style.hide(['player_id','player_name','player_display_name','position','position_group','headshot_url','season','season_type'], axis="columns").hide(axis="index").set_table_attributes({'border-collapse' : 'collapse','border-spacing' : '0px'}).set_table_styles([{'selector': 'th', 'props' : 'background-color : gainsboro; color:black; border: 2px solid black;padding : 2.5px;margin : 0 auto; font-size : 12px'}]).set_properties(**{'background-color' : 'gainsboro', 'color' :'black', 'border': '2px solid black','padding' : '2.5px','margin' : '0 auto', 'font-size' : '12px'})
    return render_template('player-stats.html', player_stats = player_data.to_html(), name = name, headshot = headshot[0], position = position[0])

@app.route('/NFL/Team/<team>/Schedule/<fullname>')
def team_schedule(team, fullname):
    file_path = os.getcwd() + '/nickknows/nfl/data/' + str(year) + '_schedule.csv'
    schedule = pd.read_csv(file_path, index_col=0)
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
    sched_path = os.getcwd() + '/nickknows/nfl/data/' + str(year) + '_schedule.csv'
    schedule = pd.read_csv(sched_path, index_col=0)
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
    sched_path = os.getcwd() + '/nickknows/nfl/data/' + str(year) + '_schedule.csv'
    schedule = pd.read_csv(sched_path, index_col=0)
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
