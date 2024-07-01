from celery import Celery

def make_celery(app=None):
    celery = Celery(
        app.import_name,
        backend="redis://redis:6379/0",
        broker="redis://redis:6379/1"
    )
    celery.conf.update(app.config)

    class ContextTask(celery.Task):
        def __call__(self, *args, **kwargs):
            with app.app_context():
                return self.run(*args, **kwargs)

    celery.Task = ContextTask
    return celery