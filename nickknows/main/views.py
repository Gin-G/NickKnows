from flask import redirect, url_for, render_template
from nickknows import app

@app.route('/')
def home():
    return render_template('home.html')
