import os
import multiprocessing
import uuid
from redis import Redis
from rq import Worker, Queue, Connection
from app import app

redis_url = os.getenv('REDIS_URL', 'redis://localhost:6379')
conn = Redis.from_url(redis_url)

def start_worker(queue_name):
    """
    Starts a dedicated worker process for a specific queue.
    """
    # Generate a unique name: "Worker-default-12345"
    unique_name = f"Worker-{queue_name}-{os.getpid()}"
    
    print(f"👷 Starting {unique_name} for '{queue_name}' queue...")
    
    with Connection(conn):
        w = Worker([Queue(queue_name)], name=unique_name)
        
        with app.app_context():
            w.work()

if __name__ == '__main__':
    print("🚀 Launching Parallel Workers...")

    # Process 1: Critical Lane
    p1 = multiprocessing.Process(target=start_worker, args=('critical',))
    
    # Process 2: Default Lane
    p2 = multiprocessing.Process(target=start_worker, args=('default',))

    p1.start()
    p2.start()
    
    p1.join()
    p2.join()
