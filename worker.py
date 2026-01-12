import os
import redis
import threading
from rq import Worker, Queue, Connection
from app import app, run_schedule

listen = ['default']

# Get Redis URL from environment
redis_url = os.getenv('REDIS_URL', 'redis://localhost:6379')

conn = redis.from_url(redis_url)

if __name__ == '__main__':
    # Start the scheduler HERE, inside the single worker process
    print("Starting Scheduler in Worker process...")
    t = threading.Thread(target=run_schedule, daemon=True)
    t.start()

    # CRITICAL FIX: Do NOT use 'with app.app_context():' here.
    # We want the worker to create a FRESH connection for every job.
    with Connection(conn):
        worker = Worker(map(Queue, listen))
        worker.work()
