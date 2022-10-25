from flask import redirect, url_for, render_template
from nickknows import app

@app.route('/')
def home():
    return render_template('home.html')

@app.route('/templates/header.html')
def header():
    return render_template('header.html')

@app.route('/templates/nfl_navbar.html')
def nfl_navbar():
    return render_template('nfl_navbar.html')