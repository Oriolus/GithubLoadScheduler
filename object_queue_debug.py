from uuid import uuid4

from LoadHandler import LoadHandler
from ObjectQueue import ObjectQueue

from main import get_logger
from config import get_config


from apscheduler.executors.pool import ThreadPoolExecutor
from apscheduler.schedulers.background import BlockingScheduler
from apscheduler.jobstores.sqlalchemy import SQLAlchemyJobStore


config = get_config()
def_logger = get_logger()
queue = ObjectQueue(config)
load_handler = LoadHandler(def_logger, config)


jobStores = {
    'default': SQLAlchemyJobStore(url='postgresql://{}:{}@{}:5432/{}'.format(
        config.sched_db.user,
        config.sched_db.password,
        config.sched_db.host,
        config.sched_db.database
    ))
}
executors = {
    'default': ThreadPoolExecutor(12)
}

scheduler = BlockingScheduler(
    jobstores=jobStores,
    executors=executors
)


def delete_ancient_entries():
    queue.delete_ancient_entries()


def fill_queue():
    queue.fill()
    def_logger.info('fill_queue')


def run_job(id: int):
    load_handler.handle(id)


def prepare_job():
    entries = queue.next_entries_by_current_timestamp()
    if len(entries) > 0:
        def_logger.info('adding jobs to execute: {}'.format(len(entries)))
    for entry in entries:
        scheduler.add_job(run_job, id=str(uuid4()), kwargs={'id': entry.id})


scheduler.add_job(prepare_job, 'interval', milliseconds=200, id='prepare_job', replace_existing=True)
scheduler.add_job(fill_queue, 'interval', seconds=30, id='fill_queue', replace_existing=True)
scheduler.add_job(delete_ancient_entries, 'interval', seconds=120, id='delete_ancient_entries', replace_existing=True)

try:
    queue.clear()
    scheduler.start()
except Exception as e:
    print(str(e))
finally:
    print('finally')
