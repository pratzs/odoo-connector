import os
import uuid
from redis import Redis
from rq import Worker, Queue
from app import app

redis_url = os.getenv('REDIS_URL', 'redis://localhost:6379')
conn = Redis.from_url(redis_url)

if __name__ == '__main__':
    unique_id = str(uuid.uuid4())[:8]
    unique_name = f"Worker-{unique_id}"

    # Single process listens to both queues; RQ checks critical before default,
    # so inventory/order jobs always run first. One process instead of two
    # saves ~100-150 MB RSS on Render's 512 MB instance.
    queues = [
        Queue('critical', connection=conn),
        Queue('default', connection=conn),
    ]
    w = Worker(queues, connection=conn, name=unique_name)

    print(f"👷 Starting {unique_name} on [critical, default] queues...")

    with app.app_context():
        w.work()
