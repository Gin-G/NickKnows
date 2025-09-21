from flask import render_template, url_for, redirect, flash, request, session
from nickknows import app
from ..celery_setup.tasks import process_team_data, update_fpa_data, update_PBP_data, update_roster_data, update_sched_data, update_week_data, update_qb_yards_top10, update_qb_tds_top10, update_rb_yards_top10, update_rb_tds_top10, update_rec_yds_top10, update_rec_tds_top10, update_team_schedule, update_weekly_team_data, update_snap_count_data, get_snap_count_summary, update_all_teams_snap_counts, update_opportunity_data
from celery import chain, chord
import nfl_data_py as nfl
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from IPython.display import HTML
import os
import json
import time
from pathlib import Path
from celery.utils.log import get_task_logger
from datetime import datetime
logger = get_task_logger(__name__)
pd.options.mode.chained_assignment = None

def get_available_years():
    """Get available NFL years - only include years where data is actually available"""
    start_year = 2020
    end_year = 2025
    return list(range(start_year, end_year + 1))

def get_selected_year():
    """Get selected year from session, request args, or default to latest available"""
    available_years = get_available_years()
    
    year_from_request = request.args.get('year', type=int)
    if year_from_request and year_from_request in available_years:
        session['selected_nfl_year'] = year_from_request
        return year_from_request
    
    year_from_session = session.get('selected_nfl_year')
    if year_from_session and year_from_session in available_years:
        return year_from_session
    
    default_year = max(available_years)
    session['selected_nfl_year'] = default_year
    return default_year

def get_season_display_name(year):
    """Format NFL season display name"""
    return f"{year} Season"

@app.route('/NFL/set_year/<int:year>')
def set_nfl_year(year):
    """Set the NFL year in session and redirect back to referrer or NFL home"""
    available_years = get_available_years()
    if year in available_years:
        session['selected_nfl_year'] = year
        flash(f'Season set to {year}')
    else:
        flash(f'Invalid year: {year}')
    
    return redirect(request.referrer or url_for('NFL'))

@app.route('/NFL')
def NFL():
    available_years = get_available_years()
    selected_year = get_selected_year()
    current_season = max(available_years)  # 2025 currently
    base_path = os.getcwd() + '/nickknows/nfl/data/' + str(selected_year)
    
    # Only check for updates if this is the current season
    update_needed = False
    if selected_year == current_season:
        # Check if current season data needs updating (7 day threshold)
        core_files = [
            f'{base_path}_pbp_data.csv',
            f'{base_path}_rosters.csv', 
            f'{base_path}_schedule.csv',
            f'{base_path}_weekly_data.csv'
        ]
        
        week_threshold = 7 * 24 * 60 * 60  # 7 days in seconds
        current_time = time.time()
        
        for file_path in core_files:
            if not os.path.exists(file_path):
                update_needed = True
                break
            elif (current_time - os.path.getmtime(file_path)) > week_threshold:
                update_needed = True
                break
    else:
        # Historical season - just check if data exists at all
        core_files = [
            f'{base_path}_pbp_data.csv',
            f'{base_path}_rosters.csv', 
            f'{base_path}_schedule.csv',
            f'{base_path}_weekly_data.csv'
        ]
        
        # Only update if data is completely missing
        for file_path in core_files:
            if not os.path.exists(file_path):
                update_needed = True
                break

    if update_needed:
        # Trigger all updates with selected year
        update_tasks = [
            (update_PBP_data, selected_year), 
            (update_roster_data, selected_year), 
            (update_sched_data, selected_year), 
            (update_week_data, selected_year),
            (update_qb_yards_top10, selected_year), 
            (update_qb_tds_top10, selected_year), 
            (update_rb_yards_top10, selected_year),
            (update_rb_tds_top10, selected_year), 
            (update_rec_yds_top10, selected_year), 
            (update_rec_tds_top10, selected_year)
        ]
        for task, year in update_tasks:
            task.delay(year)
        
        if selected_year == current_season:
            flash('Current season data is updating in the background. Refresh the page in a bit')
        else:
            flash(f'{selected_year} season data is being loaded for the first time. Refresh the page in a bit')
        
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
    # Pass year to all update tasks
    update_PBP_data.delay(selected_year)
    update_roster_data.delay(selected_year)
    update_sched_data.delay(selected_year)
    update_week_data.delay(selected_year)
    time.sleep(30)
    update_qb_yards_top10.delay(selected_year)
    update_qb_tds_top10.delay(selected_year)
    update_rb_yards_top10.delay(selected_year)
    update_rb_tds_top10.delay(selected_year)
    update_rec_yds_top10.delay(selected_year)
    update_rec_tds_top10.delay(selected_year)
    flash('All data is updating in the background. Changes should be reflected on the pages shortly')
    return redirect(url_for('NFL', year=selected_year))

@app.route('/NFL/FPA/update')
def FPAupdate():
    selected_year = get_selected_year()
    teams = ['ARI','ATL','BAL','BUF','CAR','CHI','CIN','CLE','DAL','DEN','DET','GB','HOU','IND','JAX','KC','LA','LAC','LV','MIA','MIN','NE','NO','NYG','NYJ','PHI','PIT','SEA','SF','TB','TEN','WAS']
    
    # Create chains for each team that include graph generation with year
    team_chains = []
    for team in teams:
        team_chains.append(
            chain(
                update_team_schedule.si(team, selected_year),
                update_weekly_team_data.si(team, selected_year),
                process_team_data.si(team, selected_year)
            )
        )
    
    # Execute all chains in parallel and collect results
    chord(team_chains)(update_fpa_data.s(selected_year))
    
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
        
        # Update URL to include year parameter
        url = f'<a href="https://www.nickknows.net/NFL/PbP/{week_schedule["game_id"]}?year={selected_year}">' + \
              week_schedule['away_team'] + ' vs. ' + week_schedule['home_team'] + '</a>'
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
        flash(f'Schedule data for {selected_year} not found. Please update data.')
        return redirect(url_for('NFL', year=selected_year))

@app.route('/NFL/Roster/<team>/<fullname>')
def roster(team,fullname):
    try:
        selected_year = get_selected_year()
        available_years = get_available_years()
        file_path = os.getcwd() + '/nickknows/nfl/data/' + str(selected_year) + '_rosters.csv'
        roster_data = pd.read_csv(file_path, index_col=0)
        team_roster = roster_data.loc[roster_data['team'] == team]

        # Drop duplicates based on player identifying information
        team_roster = team_roster.drop_duplicates(
            subset=['player_name', 'jersey_number'], 
            keep='first'
        )

        # Update player links to include year
        url = f'<a href="https://www.nickknows.net/NFL/Player/{team_roster["player_name"]}?year={selected_year}">' + \
              team_roster['player_name'] + '</a>'
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
                             team_roster = team_roster.to_html(classes="table", escape=False), 
                             team = fullname,
                             years=available_years,
                             selected_year=selected_year)
    except FileNotFoundError as e:
        flash(f'Roster data for {fullname} ({selected_year}) not found. Please update data.')
        return redirect(url_for('NFL', year=selected_year))

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
        flash(f'Play-by-play data for {selected_year} not found.')
        return redirect(url_for('NFL', year=selected_year))

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
        try:
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
        except:
            flash(f'Player data for {name} in {selected_year} not found.')
            return redirect(url_for('NFL', year=selected_year))

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
        flash(f'FPA data for {selected_year} not found. Please update team data.')
        return redirect(url_for('NFL', year=selected_year))

@app.route('/NFL/Team/Schedule/<team>/<fullname>')
def team_schedule(team, fullname):
    try:
        selected_year = get_selected_year()
        available_years = get_available_years()
        team_dir = os.getcwd() + '/nickknows/nfl/data/' + team + '/'
        file_path = team_dir + str(selected_year) + '_' + team + '_schedule.csv'
        
        if not os.path.exists(file_path):
            update_team_schedule.delay(team, selected_year)
            flash(f'Team schedule for {fullname} ({selected_year}) is updating. Please refresh in a moment.')
            return redirect(url_for('NFL', year=selected_year))
        
        team_schedule_data = pd.read_csv(file_path, index_col=0)
        team_schedule_data = team_schedule_data.style.hide(axis="index")
        
        return render_template('team-schedule.html', 
                             team_schedule=team_schedule_data.to_html(classes="table", escape=False), 
                             fullname=fullname,
                             years=available_years,
                             selected_year=selected_year)
    except Exception as e:
        flash(f'Error loading team schedule for {fullname} ({selected_year}): {str(e)}')
        return redirect(url_for('NFL', year=selected_year))

@app.route('/NFL/Team/Results/<team>/<fullname>')
def team_results(team, fullname):
    try:
        selected_year = get_selected_year()
        available_years = get_available_years()
        team_dir = os.getcwd() + '/nickknows/nfl/data/' + team + '/'
        file_path = team_dir + str(selected_year) + '_' + team + '_data.csv'
        
        if not os.path.exists(file_path):
            update_weekly_team_data.delay(team, selected_year)
            flash(f'Team results for {fullname} ({selected_year}) are updating. Please refresh in a moment.')
            return redirect(url_for('NFL', year=selected_year))
        
        team_data = pd.read_csv(file_path, index_col=0)
        team_data = team_data.style.hide(axis="index")
        
        return render_template('team-results.html', 
                             team_results=team_data.to_html(classes="table"), 
                             fullname=fullname,
                             years=available_years,
                             selected_year=selected_year)
    except Exception as e:
        flash(f'Error loading team results for {fullname} ({selected_year}): {str(e)}')
        return redirect(url_for('NFL', year=selected_year))

@app.route('/NFL/Team/FPA/<team>/<fullname>')
def team_fpa(team, fullname):
    try:
        selected_year = get_selected_year()
        available_years = get_available_years()
        team_dir = os.getcwd() + '/nickknows/nfl/data/' + team + '/'
        file_path = team_dir + str(selected_year) + '_' + team + '_data.csv'
        
        if not os.path.exists(file_path):
            process_team_data.delay(team, selected_year)
            flash(f'Team FPA data for {fullname} ({selected_year}) is updating. Please refresh in a moment.')
            return redirect(url_for('NFL', year=selected_year))
        
        team_data = pd.read_csv(file_path, index_col=0)
        
        # Calculate position aggregates with error handling
        pass_agg = team_data[team_data['position'] == 'QB']['fantasy_points_ppr'].mean() if len(team_data[team_data['position'] == 'QB']) > 0 else 0
        rush_agg = team_data[team_data['position'] == 'RB']['fantasy_points_ppr'].mean() if len(team_data[team_data['position'] == 'RB']) > 0 else 0
        rec_agg = team_data[team_data['position'] == 'WR']['fantasy_points_ppr'].mean() if len(team_data[team_data['position'] == 'WR']) > 0 else 0
        te_agg = team_data[team_data['position'] == 'TE']['fantasy_points_ppr'].mean() if len(team_data[team_data['position'] == 'TE']) > 0 else 0
        
        # Break down by position for detailed tables
        pass_data = team_data[team_data['position'] == 'QB'].style.hide(axis="index")
        rush_data = team_data[team_data['position'] == 'RB'].style.hide(axis="index")
        rec_data = team_data[team_data['position'] == 'WR'].style.hide(axis="index")
        te_data = team_data[team_data['position'] == 'TE'].style.hide(axis="index")
        
        # Overall team FPA summary
        team_fpa_summary = pd.DataFrame({
            'Position': ['QB', 'RB', 'WR', 'TE'],
            'Avg Fantasy Points Against': [pass_agg, rush_agg, rec_agg, te_agg]
        }).style.hide(axis="index")
        
        return render_template('team-fpa.html',
                             team_fpa=team_fpa_summary.to_html(classes="table"),
                             pass_data=pass_data.to_html(classes="table"),
                             rush_data=rush_data.to_html(classes="table"),
                             rec_data=rec_data.to_html(classes="table"),
                             te_data=te_data.to_html(classes="table"),
                             pass_agg=pass_agg,
                             rush_agg=rush_agg,
                             rec_agg=rec_agg,
                             te_agg=te_agg,
                             fullname=fullname,
                             team=team,
                             years=available_years,
                             selected_year=selected_year)
    except Exception as e:
        flash(f'Error loading team FPA data for {fullname} ({selected_year}): {str(e)}')
        return redirect(url_for('NFL', year=selected_year))
    
@app.route('/NFL/SnapCounts')
def snap_counts_home():
    """Enhanced snap counts overview page with team colors and logos"""
    available_years = get_available_years()
    selected_year = get_selected_year()
    
    try:
        # Import team description data for colors and logos
        team_desc = nfl.import_team_desc()
        
        # Create team objects with all needed data
        def create_team_data(row):
            return {
                'abbr': row['team_abbr'],
                'name': row['team_name'],
                'division': f"{row['team_conf']} {row['team_division']}",
                'primary_color': row['team_color'] if pd.notna(row['team_color']) else '#333333',
                'secondary_color': row['team_color2'] if pd.notna(row['team_color2']) else None,
                'logo': row['team_logo_squared'] if pd.notna(row['team_logo_squared']) else row['team_logo_espn']
            }
        
        # Organize teams by division
        afc_east = []
        afc_north = []
        afc_south = []
        afc_west = []
        nfc_east = []
        nfc_north = []
        nfc_south = []
        nfc_west = []
        
        # Map current team abbreviations to those in team_desc
        team_mapping = {
            'LV': 'OAK',  # Las Vegas Raiders were Oakland
            'LAC': 'SD',  # Los Angeles Chargers were San Diego
            'LA': 'LAR'   # Los Angeles Rams
        }
        
        current_teams = get_all_teams()
        
        for team_abbr in current_teams:
            # Use mapping if available, otherwise use the team abbreviation as-is
            lookup_abbr = team_mapping.get(team_abbr, team_abbr)
            
            # Find team in description data
            team_row = team_desc[team_desc['team_abbr'] == lookup_abbr]
            
            if team_row.empty:
                # Fallback for teams not found
                team_data = {
                    'abbr': team_abbr,
                    'name': get_team_fullname(team_abbr),
                    'division': 'Unknown',
                    'primary_color': '#333333',
                    'secondary_color': None,
                    'logo': 'https://via.placeholder.com/60x60?text=' + team_abbr
                }
            else:
                team_data = create_team_data(team_row.iloc[0])
                team_data['abbr'] = team_abbr  # Use current abbreviation for URLs
                team_data['name'] = get_team_fullname(team_abbr)  # Use our full name mapping
            
            # Sort into divisions based on current team abbreviations
            if team_abbr in ['BUF', 'MIA', 'NE', 'NYJ']:
                afc_east.append(team_data)
            elif team_abbr in ['BAL', 'CIN', 'CLE', 'PIT']:
                afc_north.append(team_data)
            elif team_abbr in ['HOU', 'IND', 'JAX', 'TEN']:
                afc_south.append(team_data)
            elif team_abbr in ['DEN', 'KC', 'LV', 'LAC']:
                afc_west.append(team_data)
            elif team_abbr in ['DAL', 'NYG', 'PHI', 'WAS']:
                nfc_east.append(team_data)
            elif team_abbr in ['CHI', 'DET', 'GB', 'MIN']:
                nfc_north.append(team_data)
            elif team_abbr in ['ATL', 'CAR', 'NO', 'TB']:
                nfc_south.append(team_data)
            elif team_abbr in ['ARI', 'LA', 'SEA', 'SF']:
                nfc_west.append(team_data)
        
        # Sort teams within each division alphabetically
        for division in [afc_east, afc_north, afc_south, afc_west, 
                        nfc_east, nfc_north, nfc_south, nfc_west]:
            division.sort(key=lambda x: x['name'])
        
        return render_template('snap-counts-home.html',
                             years=available_years,
                             selected_year=selected_year,
                             afc_east=afc_east,
                             afc_north=afc_north,
                             afc_south=afc_south,
                             afc_west=afc_west,
                             nfc_east=nfc_east,
                             nfc_north=nfc_north,
                             nfc_south=nfc_south,
                             nfc_west=nfc_west)
                             
    except Exception as e:
        logger.error(f"Error loading team descriptions: {str(e)}")
        # Fallback to simple team list if team_desc fails
        teams = get_all_teams()
        simple_teams = []
        
        for team in teams:
            simple_teams.append({
                'abbr': team,
                'name': get_team_fullname(team),
                'division': 'NFL',
                'primary_color': '#333333',
                'secondary_color': None,
                'logo': f'https://via.placeholder.com/60x60?text={team}'
            })
        
        return render_template('snap-counts-home.html',
                             years=available_years,
                             selected_year=selected_year,
                             teams=simple_teams,
                             error=True)

@app.route('/NFL/SnapCounts/<team>/<fullname>')
def team_snap_counts(team, fullname):
    """Team-specific snap counts page with position-specific grouping"""
    try:
        available_years = get_available_years()
        selected_year = get_selected_year()
        
        # Ensure team directory exists
        team_dir = os.getcwd() + f'/nickknows/nfl/data/{team}/'
        if not os.path.exists(team_dir):
            os.makedirs(team_dir)
        
        # Try to load cached snap count data
        snap_file_path = os.path.join(team_dir, f'{selected_year}_{team}_snap_counts.csv')
        
        if not os.path.exists(snap_file_path):
            # Trigger data update
            update_snap_count_data.delay(team, selected_year)
            flash(f'Snap count data for {fullname} is being updated. Please refresh in a moment.')
            return render_template('snap-counts-team.html',
                                 team=team,
                                 fullname=fullname,
                                 years=available_years,
                                 selected_year=selected_year,
                                 position_groups=None,
                                 loading=True)
        
        # Load and process snap count data
        try:
            snap_data = pd.read_csv(snap_file_path)
            
            # Check if the DataFrame is empty
            if snap_data.empty:
                flash(f'No snap count data available for {fullname} in {selected_year}')
                return render_template('snap-counts-team.html',
                                     team=team,
                                     fullname=fullname,
                                     years=available_years,
                                     selected_year=selected_year,
                                     position_groups=None,
                                     loading=False)
            
            # Get all available weeks and sort them
            all_weeks = sorted(snap_data['week'].unique()) if 'week' in snap_data.columns else []
            
            # Define specific position groups
            position_groups_config = [
                {
                    'name': 'Quarterbacks',
                    'positions': ['QB'],
                    'snap_type': 'offense',
                    'icon': 'fas fa-user-tie'
                },
                {
                    'name': 'Running Backs',
                    'positions': ['RB', 'FB'],
                    'snap_type': 'offense',
                    'icon': 'fas fa-running'
                },
                {
                    'name': 'Wide Receivers',
                    'positions': ['WR'],
                    'snap_type': 'offense',
                    'icon': 'fas fa-route'
                },
                {
                    'name': 'Tight Ends',
                    'positions': ['TE'],
                    'snap_type': 'offense',
                    'icon': 'fas fa-hands'
                },
                {
                    'name': 'Offensive Line',
                    'positions': ['C', 'G', 'T', 'OL'],
                    'snap_type': 'offense',
                    'icon': 'fas fa-shield-alt'
                },
                {
                    'name': 'Defensive Line',
                    'positions': ['DE', 'DT', 'NT'],
                    'snap_type': 'defense',
                    'icon': 'fas fa-fist-raised'
                },
                {
                    'name': 'Linebackers',
                    'positions': ['LB', 'ILB', 'OLB', 'MLB'],
                    'snap_type': 'defense',
                    'icon': 'fas fa-user-shield'
                },
                {
                    'name': 'Defensive Backs',
                    'positions': ['CB', 'S', 'FS', 'SS', 'DB'],
                    'snap_type': 'defense',
                    'icon': 'fas fa-eye'
                }
            ]
            
            def process_position_group(group_config):
                """Process a specific position group"""
                group_data = []
                positions = group_config['positions']
                snap_type = group_config['snap_type']
                
                for position in positions:
                    pos_data = snap_data[snap_data['position'] == position]
                    if pos_data.empty:
                        continue
                    
                    # Group by player and create weekly breakdown
                    for player in pos_data['player'].unique():
                        player_data = pos_data[pos_data['player'] == player]
                        
                        # Create weekly snap breakdown
                        weekly_snaps = {}
                        total_snaps = 0
                        
                        for week in all_weeks:
                            week_data = player_data[player_data['week'] == week]
                            if not week_data.empty:
                                row = week_data.iloc[0]
                                if snap_type == 'offense':
                                    snaps = int(row.get('offense_snaps', 0)) if pd.notna(row.get('offense_snaps', 0)) else 0
                                    pct = round(float(row.get('offense_pct', 0)) * 100, 1) if pd.notna(row.get('offense_pct', 0)) else 0.0
                                else:  # defense
                                    snaps = int(row.get('defense_snaps', 0)) if pd.notna(row.get('defense_snaps', 0)) else 0
                                    pct = round(float(row.get('defense_pct', 0)) * 100, 1) if pd.notna(row.get('defense_pct', 0)) else 0.0
                                
                                weekly_snaps[week] = {'snaps': snaps, 'pct': pct}
                                total_snaps += snaps
                            else:
                                weekly_snaps[week] = {'snaps': 0, 'pct': 0.0}
                        
                        # Only include players who actually played
                        if total_snaps > 0:
                            group_data.append({
                                'player': player,
                                'position': position,
                                'weekly_snaps': weekly_snaps,
                                'total_snaps': total_snaps
                            })
                
                # Sort by total snaps descending
                group_data.sort(key=lambda x: x['total_snaps'], reverse=True)
                return group_data
            
            # Process each position group
            position_groups = []
            for group_config in position_groups_config:
                group_data = process_position_group(group_config)
                if group_data:  # Only include groups that have players
                    position_groups.append({
                        'name': group_config['name'],
                        'icon': group_config['icon'],
                        'players': group_data,
                        'count': len(group_data)
                    })
            
            # Special teams - get all players who have ST snaps
            st_data = []
            st_players = snap_data[snap_data['st_snaps'] > 0]['player'].unique() if 'st_snaps' in snap_data.columns else []
            
            for player in st_players:
                player_data = snap_data[snap_data['player'] == player]
                position = player_data['position'].iloc[0] if not player_data.empty else 'Unknown'
                
                weekly_snaps = {}
                total_snaps = 0
                
                for week in all_weeks:
                    week_data = player_data[player_data['week'] == week]
                    if not week_data.empty:
                        row = week_data.iloc[0]
                        snaps = int(row.get('st_snaps', 0)) if pd.notna(row.get('st_snaps', 0)) else 0
                        pct = round(float(row.get('st_pct', 0)) * 100, 1) if pd.notna(row.get('st_pct', 0)) else 0.0
                        weekly_snaps[week] = {'snaps': snaps, 'pct': pct}
                        total_snaps += snaps
                    else:
                        weekly_snaps[week] = {'snaps': 0, 'pct': 0.0}
                
                if total_snaps > 0:
                    st_data.append({
                        'player': player,
                        'position': position,
                        'weekly_snaps': weekly_snaps,
                        'total_snaps': total_snaps
                    })
            
            # Sort special teams by total snaps
            st_data.sort(key=lambda x: x['total_snaps'], reverse=True)
            
            # Add special teams to position groups
            if st_data:
                position_groups.append({
                    'name': 'Special Teams',
                    'icon': 'fas fa-star',
                    'players': st_data,
                    'count': len(st_data)
                })
            
            # Create summary statistics
            total_players = len(snap_data['player'].unique())
            summary_stats = {
                'total_players': total_players,
                'weeks_available': len(all_weeks),
                'position_groups': len(position_groups)
            }
            
            return render_template('snap-counts-team.html',
                                 team=team,
                                 fullname=fullname,
                                 years=available_years,
                                 selected_year=selected_year,
                                 position_groups=position_groups,
                                 weeks=all_weeks,
                                 summary_stats=summary_stats,
                                 loading=False)
                                 
        except Exception as csv_error:
            logger.error(f"Error reading CSV file {snap_file_path}: {str(csv_error)}")
            flash(f'Error reading snap count data for {fullname}. Data may be corrupted.')
            return render_template('snap-counts-team.html',
                                 team=team,
                                 fullname=fullname,
                                 years=available_years,
                                 selected_year=selected_year,
                                 position_groups=None,
                                 loading=False)
                                 
    except Exception as e:
        logger.error(f"Error loading snap counts for {team}: {str(e)}")
        flash(f'Error loading snap count data for {fullname}: {str(e)}')
        return render_template('snap-counts-team.html',
                             team=team,
                             fullname=fullname,
                             years=available_years,
                             selected_year=selected_year,
                             position_groups=None,
                             loading=False)
         
@app.route('/NFL/SnapCounts/update/<team>')
def update_team_snap_counts(team):
    """Trigger snap count data update for a specific team"""
    selected_year = get_selected_year()
    update_snap_count_data.delay(team, selected_year)
    flash(f'Snap count data for {team} is updating in the background.')
    return redirect(url_for('team_snap_counts', team=team, fullname=get_team_fullname(team)))

@app.route('/NFL/SnapCounts/api/<team>')
def snap_counts_api(team):
    """API endpoint for snap count data"""
    try:
        selected_year = get_selected_year()
        position_filter = request.args.get('positions', '').split(',') if request.args.get('positions') else None
        
        # Get formatted data
        result = get_snap_count_summary.delay(team, selected_year, position_filter).get(timeout=30)
        
        return json.dumps(result), 200, {'Content-Type': 'application/json'}
        
    except Exception as e:
        return json.dumps({
            'success': False,
            'error': str(e),
            'team': team
        }), 500, {'Content-Type': 'application/json'}

@app.route('/NFL/SnapCounts/player/<team>/<player_name>')
def player_snap_history(team, player_name):
    """Individual player snap count history"""
    try:
        available_years = get_available_years()
        selected_year = get_selected_year()
        
        # Load player's snap count history
        snap_file_path = os.getcwd() + f'/nickknows/nfl/data/{team}/{selected_year}_{team}_snap_counts.csv'
        
        if not os.path.exists(snap_file_path):
            update_snap_count_data.delay(team, selected_year)
            flash(f'Snap count data is being updated. Please refresh in a moment.')
            return redirect(url_for('team_snap_counts', team=team, fullname=get_team_fullname(team)))
        
        snap_data = pd.read_csv(snap_file_path, index_col=0)
        player_data = snap_data[snap_data['player'] == player_name]
        
        if player_data.empty:
            flash(f'No snap count data found for {player_name}')
            return redirect(url_for('team_snap_counts', team=team, fullname=get_team_fullname(team)))
        
        # Process weekly data
        weekly_snaps = []
        for _, row in player_data.iterrows():
            weekly_snaps.append({
                'week': int(row['week']),
                'opponent': row['opponent'],
                'offense_snaps': int(row['offense_snaps']) if pd.notna(row['offense_snaps']) else 0,
                'offense_pct': round(row['offense_pct'] * 100, 1) if pd.notna(row['offense_pct']) else 0.0,
                'defense_snaps': int(row['defense_snaps']) if pd.notna(row['defense_snaps']) else 0,
                'defense_pct': round(row['defense_pct'] * 100, 1) if pd.notna(row['defense_pct']) else 0.0,
                'st_snaps': int(row['st_snaps']) if pd.notna(row['st_snaps']) else 0,
                'st_pct': round(row['st_pct'] * 100, 1) if pd.notna(row['st_pct']) else 0.0,
            })
        
        weekly_snaps.sort(key=lambda x: x['week'])
        
        # Calculate season totals
        season_totals = {
            'offense_snaps': sum(w['offense_snaps'] for w in weekly_snaps),
            'defense_snaps': sum(w['defense_snaps'] for w in weekly_snaps),
            'st_snaps': sum(w['st_snaps'] for w in weekly_snaps),
        }
        season_totals['total_snaps'] = season_totals['offense_snaps'] + season_totals['defense_snaps'] + season_totals['st_snaps']
        
        return render_template('player-snap-history.html',
                             player_name=player_name,
                             team=team,
                             fullname=get_team_fullname(team),
                             position=player_data.iloc[0]['position'],
                             years=available_years,
                             selected_year=selected_year,
                             weekly_snaps=weekly_snaps,
                             season_totals=season_totals)
                             
    except Exception as e:
        logger.error(f"Error loading player snap history for {player_name}: {str(e)}")
        flash(f'Error loading snap count data for {player_name}')
        return redirect(url_for('team_snap_counts', team=team, fullname=get_team_fullname(team)))


@app.route('/NFL/SnapCounts/update_all')
def update_all_snap_counts():
    """Trigger snap count data update for all teams"""
    selected_year = get_selected_year()
    update_all_teams_snap_counts.delay(selected_year)
    flash(f'Snap count data for all teams is updating in the background for {selected_year} season.')
    return redirect(url_for('snap_counts_home', year=selected_year))


@app.route('/NFL/Opportunities')
def opportunities_home():
    """Opportunity tracking home page"""
    try:
        available_years = get_available_years()
        selected_year = get_selected_year()
        
        # Load trend data
        trend_file_path = os.getcwd() + '/nickknows/nfl/data/' + str(selected_year) + '_opportunity_trends.csv'
        
        if not os.path.exists(trend_file_path):
            update_opportunity_data.delay(selected_year)
            flash(f'Opportunity data for {selected_year} is updating. Please refresh in a moment.')
            return render_template('opportunities-home.html',
                                 years=available_years,
                                 selected_year=selected_year,
                                 loading=True)
        
        trend_data = pd.read_csv(trend_file_path)
        
        # Get key insights
        insights = {
            'trending_up_targets': get_trending_players_view(trend_data, 'targets', 'up'),
            'trending_up_carries': get_trending_players_view(trend_data, 'carries', 'up'),
            'target_leaders': get_opportunity_leaders_view(trend_data, 'targets'),
            'carry_leaders': get_opportunity_leaders_view(trend_data, 'carries'),
            'declining_opportunities': get_trending_players_view(trend_data, 'touches', 'down')
        }
        
        return render_template('opportunities-home.html',
                             years=available_years,
                             selected_year=selected_year,
                             insights=insights,
                             loading=False)
                             
    except Exception as e:
        flash(f'Error loading opportunity data: {str(e)}')
        return render_template('opportunities-home.html',
                             years=available_years,
                             selected_year=selected_year,
                             loading=False)

@app.route('/NFL/Opportunities/<team>')
def team_opportunities(team):
    """Team-specific opportunity analysis"""
    try:
        available_years = get_available_years()
        selected_year = get_selected_year()
        fullname = get_team_fullname(team)
        
        # Load opportunity data
        opp_file_path = os.getcwd() + '/nickknows/nfl/data/' + str(selected_year) + '_opportunity_data.csv'
        trend_file_path = os.getcwd() + '/nickknows/nfl/data/' + str(selected_year) + '_opportunity_trends.csv'
        
        if not os.path.exists(opp_file_path) or not os.path.exists(trend_file_path):
            update_opportunity_data.delay(selected_year)
            flash(f'Opportunity data for {fullname} is updating. Please refresh in a moment.')
            return render_template('team-opportunities.html',
                                 team=team,
                                 fullname=fullname,
                                 years=available_years,
                                 selected_year=selected_year,
                                 loading=True)
        
        opportunity_data = pd.read_csv(opp_file_path)
        trend_data = pd.read_csv(trend_file_path)
        
        # Filter to team
        team_opportunities = opportunity_data[opportunity_data['team'] == team]
        team_trends = trend_data[trend_data['team'] == team]
        
        # Group by position
        position_data = {}
        for position in ['QB', 'RB', 'WR', 'TE']:
            pos_trends = team_trends[team_trends['position'] == position]
            if len(pos_trends) > 0:
                position_data[position] = pos_trends.sort_values('targets_avg' if position in ['QB', 'WR', 'TE'] else 'carries_avg', ascending=False)
        
        return render_template('team-opportunities.html',
                             team=team,
                             fullname=fullname,
                             years=available_years,
                             selected_year=selected_year,
                             position_data=position_data,
                             loading=False)
                             
    except Exception as e:
        flash(f'Error loading team opportunity data: {str(e)}')
        return render_template('team-opportunities.html',
                             team=team,
                             fullname=fullname,
                             years=available_years,
                             selected_year=selected_year,
                             loading=False)

@app.route('/NFL/Opportunities/update')
def update_opportunities():
    """Trigger opportunity data update"""
    selected_year = get_selected_year()
    update_opportunity_data.delay(selected_year)
    flash(f'Opportunity data for {selected_year} is updating in the background.')
    return redirect(url_for('opportunities_home'))

def get_trending_players_view(trend_data, metric, direction='up', min_trend=20, min_avg=1):
    """Get trending players for view"""
    trend_col = f'{metric}_trend'
    avg_col = f'{metric}_avg'
    
    if direction == 'up':
        filters = (
            (trend_data[trend_col] >= min_trend) &
            (trend_data[avg_col] >= min_avg)
        )
        sort_ascending = False
    else:  # direction == 'down'
        filters = (
            (trend_data[trend_col] <= -min_trend) &
            (trend_data[avg_col] >= min_avg)
        )
        sort_ascending = True
    
    result = trend_data[filters].copy()
    if len(result) > 0:
        result = result.sort_values(trend_col, ascending=sort_ascending)
        
        # Select and format columns
        display_cols = ['player_name', 'position', 'team', 'weeks_played', avg_col, f'{metric}_latest', trend_col]
        result = result[display_cols].head(10)
        result = result.round(2)
        
        return result.style.hide(axis="index").to_html(classes="table", escape=False)
    return "<p>No players meet criteria</p>"

def get_opportunity_leaders_view(trend_data, metric, min_weeks=2):
    """Get opportunity leaders for view"""
    avg_col = f'{metric}_avg'
    
    filters = (
        (trend_data['weeks_played'] >= min_weeks) &
        (trend_data[avg_col] > 0)
    )
    
    leaders = trend_data[filters].copy()
    if len(leaders) > 0:
        leaders = leaders.sort_values(avg_col, ascending=False)
        
        # Select and format columns
        display_cols = ['player_name', 'position', 'team', 'weeks_played', avg_col, f'{metric}_latest', f'{metric}_max']
        leaders = leaders[display_cols].head(15)
        leaders = leaders.round(2)
        
        return leaders.style.hide(axis="index").to_html(classes="table", escape=False)
    return "<p>No leaders available</p>"

def get_season_display_name(year):
    """Format season display name"""
    return f"{year-1}-{year} Season"
    
# Helper function to get team full names
def get_team_fullname(team_code):
    """Convert team code to full name"""
    team_names = {
        'ARI': 'Arizona Cardinals', 'ATL': 'Atlanta Falcons', 'BAL': 'Baltimore Ravens',
        'BUF': 'Buffalo Bills', 'CAR': 'Carolina Panthers', 'CHI': 'Chicago Bears',
        'CIN': 'Cincinnati Bengals', 'CLE': 'Cleveland Browns', 'DAL': 'Dallas Cowboys',
        'DEN': 'Denver Broncos', 'DET': 'Detroit Lions', 'GB': 'Green Bay Packers',
        'HOU': 'Houston Texans', 'IND': 'Indianapolis Colts', 'JAX': 'Jacksonville Jaguars',
        'KC': 'Kansas City Chiefs', 'LA': 'Los Angeles Rams', 'LAC': 'Los Angeles Chargers',
        'LV': 'Las Vegas Raiders', 'MIA': 'Miami Dolphins', 'MIN': 'Minnesota Vikings',
        'NE': 'New England Patriots', 'NO': 'New Orleans Saints', 'NYG': 'New York Giants',
        'NYJ': 'New York Jets', 'PHI': 'Philadelphia Eagles', 'PIT': 'Pittsburgh Steelers',
        'SEA': 'Seattle Seahawks', 'SF': 'San Francisco 49ers', 'TB': 'Tampa Bay Buccaneers',
        'TEN': 'Tennessee Titans', 'WAS': 'Washington Commanders'
    }
    return team_names.get(team_code, team_code)

def get_all_teams():
    """Get list of all NFL teams"""
    return ['ARI', 'ATL', 'BAL', 'BUF', 'CAR', 'CHI', 'CIN', 'CLE', 'DAL', 'DEN', 
            'DET', 'GB', 'HOU', 'IND', 'JAX', 'KC', 'LA', 'LAC', 'LV', 'MIA', 
            'MIN', 'NE', 'NO', 'NYG', 'NYJ', 'PHI', 'PIT', 'SEA', 'SF', 'TB', 'TEN', 'WAS']

# Helper functions
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