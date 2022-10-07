from glob import escape
from flask import render_template
from nickknows import app
import nfl_data_py as nfl
import pandas as pd
from IPython.display import HTML
pd.options.mode.chained_assignment = None

@app.route('/NFL')
def NFL():
    return render_template('nfl-home.html')

@app.route('/NFL/schedule/<week>')
def schedule(week):
    schedule = nfl.import_schedules([2022])
    week_schedule = schedule.loc[schedule['week'] == int(week)]
    url = str('<a href="http://127.0.0.1:5000/NFL/PbP/') + week_schedule['game_id'] + str('">') + week_schedule['game_id'] + str('</a>')
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
    roster_data = nfl.import_rosters([2022])
    team_roster = roster_data.loc[roster_data['team'] == team]
    url = str('<a href="http://127.0.0.1:5000/NFL/Player/') + team_roster['player_name'] + str('">') + team_roster['player_name'] + str('</a>')
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
    pbp_data = nfl.import_pbp_data([2022])
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
    weekly_data = nfl.import_weekly_data([2022])
    player_data = weekly_data.loc[weekly_data['player_display_name'] == name]
    player_data = player_data.style.hide(axis="index")
    player_data = player_data.set_table_attributes({'border-collapse' : 'collapse','border-spacing' : '0px'})
    player_data = player_data.set_table_styles([{'selector': 'th', 'props' : 'background-color : gainsboro; color:black; border: 2px solid black;padding : 2.5px;margin : 0 auto; font-size : 12px'}])
    player_data = player_data.set_properties(**{'background-color' : 'gainsboro', 'color' :'black', 'border': '2px solid black','padding' : '2.5px','margin' : '0 auto', 'font-size' : '12px'})
    return render_template('player-stats.html', player_stats = player_data.to_html(), name = name)

@app.route('/NFL/Team/<team>/Schedule/<fullname>')
def team_schedule(team, fullname):
    schedule = nfl.import_schedules([2022])
    url = str('<a href="http://127.0.0.1:5000/NFL/PbP/') + schedule['game_id'] + str('">') + schedule['game_id'] + str('</a>')
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
    schedule = nfl.import_schedules([2022])
    url = str('<a href="http://127.0.0.1:5000/NFL/PbP/') + schedule['game_id'] + str('">') + schedule['game_id'] + str('</a>')
    schedule['game_id'] = url
    home_team_schedule = schedule.loc[schedule['home_team'] == team]
    away_team_schedule = schedule.loc[schedule['away_team'] == team]
    full_schedule = [home_team_schedule, away_team_schedule]
    full_schedule = pd.concat(full_schedule)
    full_schedule = full_schedule.dropna(subset=['away_score'])
    full_schedule = full_schedule.sort_values(by=['week'])
    full_schedule = full_schedule.style.hide(axis="index").set_table_attributes({'border-collapse' : 'collapse','border-spacing' : '0px'}).set_table_styles([{'selector': 'th', 'props' : 'background-color : gainsboro; color:black; border: 2px solid black;padding : 2.5px;margin : 0 auto; font-size : 12px'}]).set_properties(**{'background-color' : 'gainsboro', 'color' :'black', 'border': '2px solid black','padding' : '2.5px','margin' : '0 auto', 'font-size' : '12px'}).hide(['season','game_type','location','old_game_id','gsis','nfl_detail_id','pfr','pff','espn','away_qb_id','home_qb_id','stadium_id'], axis="columns")
    
    return render_template('team-schedule.html', team_schedule = full_schedule.to_html(), fullname = fullname)