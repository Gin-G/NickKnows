# NickKnows

## Hosting Code to run my personal website

Built on GCP utilizing Python, Flask, Nginx, and Gunicorn. <br>
[Nick Knows](https://www.nickknows.net)

## Docker

To run this all on docker the following needs to be run
```
docker network create nickknows
docker run --network nickknows --name redis redis
docker run --network nickknows ncging/nickknows-celery:12-27
docker run --network nickknows -p 8000:8000 ncging/nickknows:12-27
```

## Kubernetes

There is a Helm chart in `helm_knows/` to deploy this entire application