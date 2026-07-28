from flask import Flask
from flask.templating import render_template
from celery import Celery
import os

REDIS_ENV = os.environ.get('REDIS_ENV', 'redis')

app = Flask(__name__)
app.config["CELERY_BROKER_URL"] = 'redis://' + REDIS_ENV + ':6379'
app.config["CELERY_RESULT_BACKEND"] = 'redis://' + REDIS_ENV + ':6379'
app.config["SECRET_KEY"] = "celery blooody nick ski movie music know nfl"
# In-cluster Service DNS by default; the public hostname is behind a
# Cloudflare bot-challenge that 403s server-to-server requests. Override
# via env for local dev.
app.config["NFL_API_URL"] = os.environ.get(
    'NFL_API_URL', 'http://nfl-api.nfl-api.svc.cluster.local:8000'
)
celery = Celery(app.name, broker=app.config["CELERY_BROKER_URL"])
celery.conf.update(app.config)

from nickknows.main import views
from nickknows.nfl import views
from nickknows.fahrtbags import views
from nickknows.hydrow import views
