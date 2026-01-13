import os, time, json, random
from datetime import datetime, timezone
from kafka import KafkaProducer

BROKER = os.getenv("BROKER", "redpanda:9092")
TOPIC = os.getenv("TOPIC", "client_tickets")

MSG_BYTES = int(os.getenv("MSG_BYTES", "10240"))          # 10 KB
TARGET_GB_MONTH = float(os.getenv("TARGET_GB_MONTH", "50"))
SPEEDUP = float(os.getenv("SPEEDUP", "1"))                # >1 pour accélérer la simu

SECONDS_PER_MONTH = 30 * 24 * 3600
target_bytes_per_sec = (TARGET_GB_MONTH * 1_000_000_000) / SECONDS_PER_MONTH
msgs_per_sec = max(target_bytes_per_sec / MSG_BYTES, 0.001)
sleep_s = 1.0 / (msgs_per_sec * SPEEDUP)

producer = KafkaProducer(
    bootstrap_servers=BROKER,
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    acks=1,
)

ticket_id = 1
requests = [
    ("Problème de connexion", "support"),
    ("Demande de remboursement", "billing"),
    ("Bug sur la page paiement", "incident"),
    ("Question sur abonnement", "support"),
]

while True:
    req, req_type = random.choice(requests)
    payload = {
        "ticket_id": ticket_id,
        "client_id": random.randint(1, 5000),
        "created_at": datetime.now(timezone.utc).isoformat(),
        "request": req,
        "request_type": req_type,
        "priority": random.choice(["low", "medium", "high"]),
    }

    # Padding pour atteindre ~MSG_BYTES (approx OK pour une simu)
    raw = json.dumps(payload).encode("utf-8")
    pad_len = max(MSG_BYTES - len(raw), 0)
    payload["padding"] = "x" * pad_len

    producer.send(TOPIC, payload)
    ticket_id += 1
    time.sleep(sleep_s)
