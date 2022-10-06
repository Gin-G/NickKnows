from flask import render_template
from nickknows import app
import nfl_data_py as nfl

@app.route('/NFL')
def NFL():
    return render_template('nfl-home.html')

@app.route('/NFL/schedule/<week>')
def schedule(week):
    schedule = nfl.import_schedules([2022])
    week_schedule = schedule.loc[schedule['week'] == int(week)]
    return week_schedule.to_html()

@app.route('/NFL/Roster/<team>')
def roster(team):
    roster_data = nfl.import_rosters([2022])
    team_roster = roster_data.loc[roster_data['team'] == team]
    return team_roster.to_html()