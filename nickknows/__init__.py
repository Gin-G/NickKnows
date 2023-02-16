import imp
from flask import Flask
from flask.templating import render_template
from .celery_setup.celery import make_celery

app = Flask(__name__)
celery = make_celery(app)

from nickknows.main import views
from nickknows.nfl import views
from nickknows.fahrtbags import views
