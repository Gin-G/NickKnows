from flask import render_template, url_for, redirect, flash, request
from nickknows import app
from ..celery_setup.tasks import process_team_data, update_fpa_data, update_PBP_data, update_roster_data, update_sched_data, update_week_data, update_qb_yards_top10, update_qb_tds_top10, update_rb_yards_top10, update_rb_tds_top10, update_rec_yds_top10, update_rec_tds_top10, update_team_schedule, update_weekly_team_data
from celery import chain, chord
import nfl_data_py as nfl
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from IPython.display import HTML
import os
import time
from pathlib import Path
from celery.utils.log import get_task_logger
from datetime import datetime
logger = get_task_logger(__name__)
pd.options.mode.chained_assignment = None

# Dynamic year calculation
def get_available_years():
    """Get available NFL years from 2020 to current year + 1"""
    current_year = datetime.now().year
    # NFL season typically runs Sep-Feb, so include next year if we're in NFL season
    end_year = current_year + 1 if datetime.now().month >= 9 else current_year
    return list(range(2020, end_year + 1))

def get_selected_year():
    """Get selected year from request args or default to latest available"""
    available_years = get_available_years()
    selected = request.args.get('year', max(available_years), type=int)
    return selected if selected in available_years else max(available_years)

@app.route('/NFL')
def NFL():
    available_years = get_available_years()
    selected_year = get_selected_year()
    base_path = os.getcwd() + '/nickknows/nfl/data/' + str(selected_year)
    
    # Check if update is needed
    last_update = os.environ.get(f'NFL_DATA_LAST_UPDATE_{selected_year}')
    current_time = datetime.now()
    update_needed = not last_update or (current_time - datetime.fromisoformat(last_update)).days >= 7

    if update_needed:
        # Trigger all updates at once
        update_tasks = [
            update_PBP_data, update_roster_data, update_sched_data, update_week_data,
            update_qb_yards_top10, update_qb_tds_top10, update_rb_yards_top10,
            update_rb_tds_top10, update_rec_yds_top10, update_rec_tds_top10
        ]
        for task in update_tasks:
            task.delay()
        os.environ[f'NFL_DATA_LAST_UPDATE_{selected_year}'] = current_time.isoformat()
        flash('Data is updating in the background. Refresh the page in a bit')
        return render_template('nfl-home.html', years=available_years, selected_year=selected_year)

    try:
        # File paths
        files = {
            'pass_agg': f'{base_path}_qb_yards_top10_data.csv',
            'pass_td_agg': f'{base_path}_qb_tds_top10_data.csv',
            'rush_yds_agg': f'{base_path}_rb_yds_top10_data.csv',
            'rush_td_agg': f'{base_path}_rb_tds_top10_data.csv',
            'rec_yds_agg': f'{base_path}_rec_yds_top10_data.csv',
            'rec_td_agg': f'{base_path}_rec_tds_top10_data.csv'
        }

        # Load data
        data = {}
        for name, path in files.items():
            if os.path.exists(path):
                df = pd.read_csv(path, index_col=0)
                data[name] = df.style.hide(axis="index").format(precision=0)
            else:
                raise FileNotFoundError(f"Missing file: {path}")

        # Render template with all data
        return render_template(
            'nfl-home.html',
            pass_yards_data=data['pass_agg'].to_html(classes="table"),
            pass_td_data=data['pass_td_agg'].to_html(),
            rush_yards_data=data['rush_yds_agg'].to_html(),
            rush_td_data=data['rush_td_agg'].to_html(),
            rec_yards_data=data['rec_yds_agg'].to_html(),
            rec_td_data=data['rec_td_agg'].to_html(),
            years=available_years,
            selected_year=selected_year
        )

    except Exception as e:
        # Reset update time to force refresh on next load
        os.environ.pop(f'NFL_DATA_LAST_UPDATE_{selected_year}', None)
        flash('Error loading data. Updates scheduled.')
        return render_template('nfl-home.html', years=available_years, selected_year=selected_year)
    
@app.route('/NFL/update')
def NFLupdate():
    selected_year = get_selected_year()
    update_PBP_data.delay()
    update_roster_data.delay()
    update_sched_data.delay()
    update_week_data.delay()
    time.sleep(30)
    update_qb_yards_top10.delay()
    update_qb_tds_top10.delay()
    update_rb_yards_top10.delay()
    update_rb_tds_top10.delay()
    update_rec_yds_top10.delay()
    update_rec_tds_top10.delay()
    flash('All data is updating in the background. Changes should be reflected on the pages shortly')
    return redirect(url_for('NFL', year=selected_year))

@app.route('/NFL/FPA/update')
def FPAupdate():
    selected_year = get_selected_year()
    teams = ['ARI','ATL','BAL','BUF','CAR','CHI','CIN','CLE','DAL','DEN','DET','GB','HOU','IND','JAX','KC','LA','LAC','LV','MIA','MIN','NE','NO','NYG','NYJ','PHI','PIT','SEA','SF','TB','TEN','WAS']
    
    # Create chains for each team that include graph generation
    team_chains = []
    for team in teams:
        team_chains.append(
            chain(
                update_team_schedule.si(team),
                update_weekly_team_data.si(team),
                process_team_data.si(team)
            )
        )
    
    # Execute all chains in parallel and collect results
    chord(team_chains)(update_fpa_data.s())
    
    flash('All team data is updating in the background. Changes should be reflected on the pages shortly')
    return redirect(url_for('NFL', year=selected_year))

@app.route('/NFL/schedule/')
def schedule():
    try:
        available_years = get_available_years()
        selected_year = get_selected_year()
        season_display = get_season_display_name(selected_year)
        week = int(request.args.get('week', '1'))
        available_weeks = range(1, 19) if selected_year >= 2021 else range(1, 18)
        file_path = os.getcwd() + '/nickknows/nfl/data/' + str(selected_year) + '_schedule.csv'
        schedule = pd.read_csv(file_path, index_col=0)
        week_schedule = schedule.loc[schedule['week'] == int(week)]
        url = str('<a href="http://nickknows.net/NFL/PbP/') + week_schedule['game_id'] + str('">') + week_schedule['away_team'] + ' vs. ' + week_schedule['home_team'] + str('</a>')
        week_schedule['game_id'] = url
        week_schedule.loc[week_schedule["overtime"] == 0, "overtime"] = "No"
        week_schedule.loc[week_schedule["overtime"] == 1, "overtime"] = "Yes"
        week_schedule.loc[week_schedule["div_game"] == 0, "div_game"] = "No"
        week_schedule.loc[week_schedule["div_game"] == 1, "div_game"] = "Yes"
        week_schedule.rename(columns = {'game_id':'Game','away_team':'Away Team','away_score':'Away Score','home_team':'Home Team','home_score':'Home Score','result':'Result','total':'Total','overtime':'Overtime','away_rest':'Away Rest','home_rest':'Home Rest','away_moneyline':'Away Moneyline','home_moneyline':'Home Moneyline','spread_line':'Spread','away_spread_odds':'Away Spread Odds','home_spread_odds':'Home Spread Odds','total_line':'Total Line','under_odds':'Under Odds','over_odds':'Over Odds','div_game':'Division Game','away_qb_name':'Away QB','home_qb_name':'Home QB','stadium':'Stadium'}, inplace=True)
        week_schedule = week_schedule.style.hide(axis="index")
        week_schedule = week_schedule.hide(['roof','gameday','weekday','gametime','season','game_type','ftn','week','location','old_game_id','gsis','nfl_detail_id','surface','temp','wind','pfr','pff','espn','away_qb_id','home_qb_id','away_coach','home_coach','referee','stadium_id'], axis="columns")
        week_schedule = week_schedule.format(subset=['Away Score','Home Score','Result','Total','Away Moneyline','Home Moneyline','Away Spread Odds','Home Spread Odds','Under Odds','Over Odds'],precision=0).format(subset=['Spread','Total Line'],precision=1)
        week_schedule.apply(lambda week_schedule: total_highlight(week_schedule, "Total", "Total Line"), axis=None)
        return render_template('weekly.html', 
                             week_schedule = HTML(week_schedule.to_html(render_links=True,escape=False,classes="table")), 
                             weeks = available_weeks, 
                             week = week, 
                             years=available_years, 
                             selected_year=selected_year,
                             season_display=season_display)
    except FileNotFoundError as e:
        flash(str(e))
        return redirect(url_for('NFL'))

@app.route('/NFL/Roster/<team>/<fullname>')
def roster(team,fullname):
    try:
        selected_year = get_selected_year()
        available_years = get_available_years()
        file_path = os.getcwd() + '/nickknows/nfl/data/' + str(selected_year) + '_rosters.csv'
        roster_data = pd.read_csv(file_path, index_col=0)
        team_roster = roster_data.loc[roster_data['team'] == team]

        # Drop duplicates based on player identifying information
        # Keep the first occurrence of each player
        team_roster = team_roster.drop_duplicates(
            subset=['player_name', 'jersey_number'], 
            keep='first'
        )

        url = str('<a href="http://nickknows.ging.nickknows.net/NFL/Player/') + team_roster['player_name'] + str('">') + team_roster['player_name'] + str('</a>')
        team_roster['player_name'] = url
        team_roster.rename(columns={
            'depth_chart_position':'Position',
            'jersey_number':'Number',
            'status':'Status',
            'player_name':'Full Name',
            'first_name':'First Name',
            'last_name':'Last Name',
            'height':'Height',
            'weight':'Weight',
            'football_name':'Preferred Name',
            'rookie_year':'Rookie Year',
            'draft_club':'Drafted By',
            'draft_number':'Draft Number'
        }, inplace=True)

        team_roster.sort_values(by=['Number'], inplace=True)
        team_roster = team_roster.style.hide(axis="index")
        team_roster = team_roster.hide([
            'season', 'team', 'position', 'birth_date', 'college',
            'player_id', 'espn_id', 'sportradar_id', 'yahoo_id',
            'rotowire_id', 'pff_id', 'pfr_id', 'fantasy_data_id',
            'sleeper_id', 'years_exp', 'headshot_url', 'ngs_position',
            'week', 'game_type', 'status_description_abbr', 'esb_id',
            'gsis_it_id', 'smart_id', 'entry_year'
        ], axis="columns")

        team_roster = team_roster.format(precision=0, na_rep="Undrafted")
        return render_template('rosters.html', 
                             team_roster = team_roster.to_html(classes="table"), 
                             team = fullname,
                             years=available_years,
                             selected_year=selected_year)
    except FileNotFoundError as e:
        flash(str(e))
        return redirect(url_for('NFL'))

# Add year context to all other NFL routes similarly...
@app.route('/NFL/PbP/<game>')
def game_pbp(game):
    try:
        selected_year = get_selected_year()
        available_years = get_available_years()
        file_path = os.getcwd() + '/nickknows/nfl/data/' + str(selected_year) + '_pbp_data.csv'
        pbp_data = pd.read_csv(file_path, index_col=0)
        game_data = pbp_data.loc[pbp_data['game_id'] == game]
        game_data.rename(columns={'posteam':'Possession','defteam':'Defense','side_of_field':'Field Side','yardline_100':'Distance from EndZone','quarter_seconds_remaining':'Seconds left in Quarter','half_seconds_remaining':'Seconds left in Half','game_seconds_remaining':'Seconds left in Game','drive':'Drive #'}, inplace=True)
        game_data = game_data.style.hide(axis="index")
        game_data = game_data.set_table_attributes({'border-collapse' : 'collapse','border-spacing' : '0px'})
        game_data = game_data.set_table_styles([{'selector': 'th', 'props' : 'background-color : gainsboro; color:black; border: 2px solid black;padding : 2.5px;margin : 0 auto; font-size : 12px'}])
        game_data = game_data.set_properties(**{'background-color' : 'gainsboro', 'color' :'black', 'border': '2px solid black','padding' : '2.5px','margin' : '0 auto', 'font-size' : '12px'})
        game_data = game_data.hide(['play_id','game_id','old_game_id','home_team','away_team','season_type','week','game_date','posteam_type','game_half','quarter_end','sp','qtr','goal_to_go','ydsnet','qb_kneel','qb_spike','qb_scramble'], axis="columns")
        return render_template('pbp.html', 
                             game_data = game_data.to_html(), 
                             game = game,
                             years=available_years,
                             selected_year=selected_year)
    except FileNotFoundError as e:
        flash(str(e))
        return redirect(url_for('NFL'))

@app.route('/NFL/Player/<name>')
def player_stats(name):
    selected_year = get_selected_year()
    available_years = get_available_years()
    try:
        file_path = os.getcwd() + '/nickknows/nfl/data/' + str(selected_year) + '_weekly_data.csv'
        weekly_data = pd.read_csv(file_path, index_col=0)
        player_data = weekly_data.loc[weekly_data['player_display_name'] == name]
        headshot = '<img src="' + player_data['headshot_url'] + '" width="360" >'
        headshot = headshot.unique()
        position = player_data['position'].unique()
        player_data.target_share = player_data.target_share * 100
        player_data.air_yards_share = player_data.air_yards_share * 100
        player_data = player_data.rename(columns={'recent_team':'Team','week':'Week','opponent_team':'Opponent','completions':'Completions','attempts':'Attempts','passing_yards':'Pass Yards','passing_tds':'Pass TDs','interceptions':'INTs','sacks':'Sacks','sack_yards':'Sack Yards','sack_fumbles':'Sack Fumbles','sack_fumbles_lost':'Lost Sack','passing_air_yards':'Air Yards','passing_yards_after_catch':'YAC','passing_first_downs':'Pass 1sts','passing_epa':'Pass EPA','passing_2pt_conversions':'Pass 2pt','pacr':'PACR','carries':'Carries','rushing_yards':'Rush Yards','rushing_tds':'Rush TD','rushing_fumbles':'Rush Fumbles','rushing_fumbles_lost':'Lost Rush','rushing_first_downs':'Rush 1sts','rushing_epa':'Rush EPA','rushing_2pt_conversions':'Rush 2pt','receptions':'Rec','targets':"Tgts",'receiving_yards':'Rec Yards','receiving_tds':'Rec TDs','receiving_fumbles':'Rec Fumble','receiving_fumbles_lost':'Lost Rec','receiving_air_yards':'Rec Air Yards','receiving_yards_after_catch':'Rec YAC','receiving_first_downs':'Rec 1sts','receiving_epa':'Rec EPA','receiving_2pt_conversions':'Rec 2pts','racr':'RACR','target_share':'Target Share','air_yards_share':'Air Yds Share','wopr':'WOPR','special_teams_tds':'Special Teams TDs','fantasy_points':'STD Points','fantasy_points_ppr':'PPR Points'})
        player_data = player_data.style.hide(['player_id','player_name','player_display_name','position','position_group','headshot_url','season','season_type','dakota'], axis="columns").hide(axis="index")
        player_data = player_data.format(subset=['Pass Yards','INTs','Sacks','Sack Yards','Air Yards','Sacks','Sack Yards','Air Yards','YAC','Pass 1sts','Rush Yards','Lost Sack','Rush Fumbles','Lost Rush','Rush 1sts','Rec Yards','Rec Fumble','Lost Rec','Rec Air Yards','Rec YAC','Rec 1sts','Special Teams TDs'],precision=0).format(subset=['Pass EPA','PACR','Rush EPA','Rec EPA','STD Points','PPR Points','RACR','WOPR','Target Share','Air Yds Share'],precision=2)
        return render_template('player-stats.html', 
                             player_stats = player_data.to_html(classes="table"), 
                             name = name, 
                             headshot = headshot[0], 
                             position = position[0],
                             years=available_years,
                             selected_year=selected_year)
    except IndexError:
        file_path = os.getcwd() + '/nickknows/nfl/data/' + str(selected_year) + '_rosters.csv'
        weekly_data = pd.read_csv(file_path, index_col=0)
        player_data = weekly_data.loc[weekly_data['player_name'] == name]
        headshot = '<img src="' + player_data['headshot_url'] + '" width="360" >'
        headshot = headshot.unique()
        position = player_data['position'].unique()
        return render_template('player-stats.html', 
                             player_stats = 'No Player Stats', 
                             name = name, 
                             headshot = headshot[0], 
                             position = position[0],
                             years=available_years,
                             selected_year=selected_year)

# Continue with other routes following the same pattern...

@app.route('/NFL/FPA')
def fpa():
    selected_year = get_selected_year()
    available_years = get_available_years()
    fpa_path = os.getcwd() + '/nickknows/nfl/data/' + str(selected_year) + '_FPA.csv'
    try:
        fpa_data = pd.read_csv(fpa_path, index_col=0)
        fpa_data.sort_values(by=['Team Name'], inplace=True)
        
        # Create plots
        cols = list(fpa_data.columns.values)
        cols.pop(0)
        fpa_data.set_index('Team Name').plot.bar(subplots=True, figsize=(8, 16), sharex=False)
        plt.tight_layout()
        plt.savefig('nickknows/static/images/FPA.png')
        
        # Style the table with color gradients for each column
        fpa_data = fpa_data.style\
            .hide(axis="index")\
            .format(precision=2)\
            .background_gradient(subset=['QB'], cmap='RdYlGn_r')\
            .background_gradient(subset=['RB'], cmap='RdYlGn_r')\
            .background_gradient(subset=['WR'], cmap='RdYlGn_r')\
            .background_gradient(subset=['TE'], cmap='RdYlGn_r')
        
        return render_template('fpa.html', 
                             fpa_data=fpa_data.to_html(classes='table'),
                             years=available_years,
                             selected_year=selected_year)
    except Exception as e:
        flash(str(e))
        return render_template('nfl-home.html', years=available_years, selected_year=selected_year)

# Helper functions remain the same
def total_highlight(df, col1, col2):
    mask = df[col1] > df[col2]
    omask = df[col1] < df[col2]
    emask = df[col1] == df[col2]
    new_df = pd.DataFrame(index=df.index, columns=df.columns)
    new_df[col1] = np.where(mask, "background-color: {}".format("green"), new_df[col1])
    new_df[col1] = np.where(omask, "background-color: {}".format("red"), new_df[col1])
    new_df[col1] = np.where(emask, "background-color: {}".format("yellow"), new_df[col1])
    return new_df

def neg_spread_highlight(df, col1, col2):
    mask = df[col1] < df[col2]
    omask = df[col1] > df[col2]
    emask = df[col1] == df[col2]
    new_df = pd.DataFrame(index=df.index, columns=df.columns)
    new_df[col1] = np.where(mask, "background-color: {}".format("green"), new_df[col1])
    new_df[col1] = np.where(omask, "background-color: {}".format("red"), new_df[col1])
    new_df[col1] = np.where(emask, "background-color: {}".format("yellow"), new_df[col1])
    return new_df