import imp
from flask import Flask
from flask.templating import render_template
from celery import Celery

app = Flask(__name__)
app.config["CELERY_BROKER_URL"] = 'redis://localhost:6379'
app.config["SECRET_KEY"] = "celery blooody nick ski movie music know nfl"
celery = Celery(app.name, broker=app.config["CELERY_BROKER_URL"])
celery.conf.update(app.config)

from nickknows.main import views
from nickknows.nfl import views
from nickknows.fahrtbags import views
