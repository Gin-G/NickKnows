from flask import redirect, url_for, render_template
from nickknows import app

@app.route('/fahrtbags')
def fart():
    return render_template('fahrtbags.html')

@app.route('/fahrtbags/one-piece')
def one_piece():
    return render_template('one-piece.html')

@app.route('/fahrtbags/about')
def about_fart():
    return render_template('about.html')
