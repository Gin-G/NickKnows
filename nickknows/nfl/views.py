from flask import render_template
from nickknows import app

@app.route('/NFL')
def NFL():
    return render_template('nfl-home.html')