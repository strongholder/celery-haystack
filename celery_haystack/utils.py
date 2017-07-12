from django.core.exceptions import ImproperlyConfigured
from django.utils.importlib import import_module
from django.db import connection as db_connection

from haystack.utils import get_identifier

from .conf import settings


def get_update_task(task_path=None):
    import_path = task_path or settings.CELERY_HAYSTACK_DEFAULT_TASK
    module, attr = import_path.rsplit('.', 1)
    try:
        mod = import_module(module)
    except ImportError as e:
        raise ImproperlyConfigured('Error importing module %s: "%s"' %
                                   (module, e))
    try:
        Task = getattr(mod, attr)
    except AttributeError:
        raise ImproperlyConfigured('Module "%s" does not define a "%s" '
                                   'class.' % (module, attr))
    return Task


def enqueue_task(action, instance):
    """
    Common utility for enqueing a task for the given action and
    model instance.
    """

    # trigger the index updating only when the database transaction has finished!
    def on_transaction_commit():
        identifier = get_identifier(instance)
        update_task = get_update_task()
        update_task.delay(action, identifier)

    on_transaction_commit.identifier = get_identifier(instance)
    on_transaction_commit.action = action

    if action == "delete":
        # clear previous tasks for the same instance from the queue; no point in executing them if it is already scheduled for deletion
        for transaction_id, func in db_connection.run_on_commit:
            if getattr(func, "identifier", None) == on_transaction_commit.identifier:
                db_connection.run_on_commit.remove((transaction_id, func, ))

    elif action == "update":
        # clear previous tasks for the same instance update operation
        for transaction_id, func in db_connection.run_on_commit:
            if getattr(func, "identifier", None) == on_transaction_commit.identifier and getattr(func, "action", None) == action:
                db_connection.run_on_commit.remove((transaction_id, func, ))

    db_connection.on_commit(on_transaction_commit)
