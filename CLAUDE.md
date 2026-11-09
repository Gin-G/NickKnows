# NickKnows — working notes

The personal site at **nickknows.net**: Flask + Gunicorn + Nginx, with Celery
and Redis behind it, deployed to Kubernetes via `helm_knows/`. The NFL section
is the part under active change; it is a **presentation layer over NFL-API**,
not a second analytics stack.

This file holds the things that are not inferable from reading the code.

## Where the NFL data comes from

`app/nickknows/nfl/nfl_api_client.py` is the only route to NFL data, and it
talks to the **in-cluster Service DNS**:

```
http://nfl-api.nfl-api.svc.cluster.local:8000
```

Not the public hostname. `nfl-api.nickknows.net` sits behind a Cloudflare
bot-challenge that 403s server-to-server requests, so a pod calling the public
host fails in a way that looks like the API being down. Override `NFL_API_URL`
for local dev against the public host.

The client caches (`NFL_API_CACHE_TTL`, default 1 hour) because the endpoints
it reads — grades, ratings, teams — change at most daily.

**The migration direction is one-way: computation moves OUT of here and into
NFL-API.** If a page needs a number this repo currently computes locally, the
default is to add or extend an NFL-API endpoint and have the client read it,
rather than growing the pandas code here. `api_frames.py` is the adapter layer
for that.

## Running the tests

```bash
cd app
NP=/home/gin-g/miniforge3/pkgs/numpy-1.26.4-py310hb13e2d6_0/lib/python3.10/site-packages
PYTHONPATH=.:$NP /home/gin-g/miniforge3/bin/python -m pytest tests/ -q
```

30 tests, ~7 s. The same interpreter constraint as the sibling repos: the system Python's
sklearn/numpy pairing is broken and there is no network for pip, so `$NP` is a
numpy 1.26 shim that has to sit ahead of it on the path.

## Deploying

Helm chart in `helm_knows/`, plus `Dockerfile` (web) and `Dockerfile.broker`
(Celery). `argo-workflow.yaml` drives the build. Two images, so a change to
shared code needs both rebuilt — a web-only rebuild leaves the worker running
the old code, which presents as a page that works on refresh and not on the
background task.

Commit and push **only when asked**.

## Conventions

- The NFL pages use their own base template (`templates/nfl-base.html`) and
  navbar; new NFL pages extend that, not the site-wide one.
- Keep the client thin. Retries, caching and error shape belong in
  `nfl_api_client.py`; views should not catch `requests` exceptions
  individually.
- `NflApiError` is the single failure type the views handle.
