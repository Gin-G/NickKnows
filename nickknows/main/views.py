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

@app.route('/templates/nick_navbar.html')
def nick_navbar():
    return render_template('navbar.html')

@app.route('/arcade')
def arcade():
    return render_template('arcade.html')

@app.route('/lemans')
def lemans():
    return render_template('lemans.html')

@app.route('/guitar')
def guitar():
    return render_template('guitar.html')

@app.route('/skiing')
def skiing():
    return render_template('skiing.html')