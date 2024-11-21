from celery import Celery

REDIS_ENV = 'localhost'

def make_celery(app=None):
    celery = Celery(
        app.import_name,
        backend="redis://" + REDIS_ENV + ":6379/0",
        broker="redis://" + REDIS_ENV + ":6379/1"
    )
    celery.conf.update(app.config)

    class ContextTask(celery.Task):
        def __call__(self, *args, **kwargs):
            with app.app_context():
                return self.run(*args, **kwargs)

    celery.Task = ContextTask
    return celery