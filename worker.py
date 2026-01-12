import os
import redis
from rq import Worker, Queue, Connection
# REMOVED: import threading, run_schedule (Moved to clock.py)

listen = ['default']

# Get Redis URL from environment
redis_url = os.getenv('REDIS_URL', 'redis://localhost:6379')

conn = redis.from_url(redis_url)

if __name__ == '__main__':
    print("👷 WORKER PROCESS STARTED")
    print("Listening for jobs on 'default' queue...")
    
    # CRITICAL: We do NOT start the scheduler thread here anymore.
    # This allows you to run 5 workers if you want, without 5 schedulers running.
    
    with Connection(conn):
        worker = Worker(map(Queue, listen))
        worker.work()
