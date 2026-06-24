import os
import uuid
from redis import Redis
from rq import Queue
from rq.worker import SimpleWorker
from app import app

redis_url = os.getenv('REDIS_URL', 'redis://localhost:6379')
conn = Redis.from_url(redis_url)

if __name__ == '__main__':
    unique_id = str(uuid.uuid4())[:8]
    unique_name = f"Worker-{unique_id}"

    # SimpleWorker runs jobs IN-PROCESS instead of forking a child process
    # per job. This saves ~150MB RSS on Render's 512MB instance because
    # fork() would create a 3rd Python process with the full app loaded.
    queues = [
        Queue('critical', connection=conn),
        Queue('default', connection=conn),
    ]
    w = SimpleWorker(queues, connection=conn, name=unique_name)

    print(f"👷 Starting {unique_name} on [critical, default] queues (SimpleWorker, no fork)...")

    with app.app_context():
        w.work()
