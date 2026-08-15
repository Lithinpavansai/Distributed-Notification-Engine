import uuid
import random
import gevent
from locust import HttpUser, task, between
from requests.adapters import HTTPAdapter

COLLISION_KEYS = [f"collision-key-{i}" for i in range(3)]

class NotificationUser(HttpUser):
    # No artificial wait time between tasks to maximise throughput

    def on_start(self):
        """Mount a large connection pool so the Locust client itself
        cannot become the bottleneck at high concurrency."""
        adapter = HTTPAdapter(pool_connections=200, pool_maxsize=200)
        self.client.mount("http://", adapter)
        self.client.mount("https://", adapter)

    @task(90)
    def post_unique_notification(self):
        user_id = f"user-{uuid.uuid4()}"
        idempotency_key = f"unique-{uuid.uuid4()}"
        payload = {
            "idempotency_key": idempotency_key,
            "title": "Unique Load Test",
            "message": "Unique notification payload"
        }
        headers = {"X-User-Id": user_id, "X-Request-Source": "unique"}
        self.client.post("/v1/notifications", json=payload, headers=headers, name="Ingest unique key")
        
    @task(10)
    def post_reused_notification(self):
        # Fire two near-simultaneous requests for the same idempotency_key from different user IDs
        user_id_1 = f"collision-user-{uuid.uuid4()}"
        user_id_2 = f"collision-user-{uuid.uuid4()}"
        idempotency_key = random.choice(COLLISION_KEYS)
        
        payload = {
            "idempotency_key": idempotency_key,
            "title": "Collision Load Test",
            "message": "Reused key payload"
        }
        
        g1 = gevent.spawn(
            self.client.post, 
            "/v1/notifications", 
            json=payload, 
            headers={"X-User-Id": user_id_1, "X-Request-Source": "reused"}, 
            name="Ingest reused key (reconciliation)"
        )
        g2 = gevent.spawn(
            self.client.post, 
            "/v1/notifications", 
            json=payload, 
            headers={"X-User-Id": user_id_2, "X-Request-Source": "reused"}, 
            name="Ingest reused key (reconciliation)"
        )
        gevent.joinall([g1, g2])
