import imp
from flask import Flask
from flask.templating import render_template

app = Flask(__name__)

from nickknows.main import views
from nickknows.nfl import views
from nickknows.fahrtbags import views
