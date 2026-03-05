import json
import os
from datetime import datetime, timezone

import pika


RABBITMQ_HOST = os.getenv("ALERT_RMQ_HOST", "rabbitmq")
RABBITMQ_PORT = int(os.getenv("ALERT_RMQ_PORT", "5672"))
RABBITMQ_USER = os.getenv("ALERT_RMQ_USER", "admin")
RABBITMQ_PASS = os.getenv("ALERT_RMQ_PASS", "admin")
RABBITMQ_VHOST = os.getenv("ALERT_RMQ_VHOST", "/")

ALERT_EXCHANGE = os.getenv("ALERT_EXCHANGE", "alerts.exchange")
ALERT_QUEUE = os.getenv("ALERT_QUEUE", "alerts.email")
ALERT_ROUTING_KEY = os.getenv("ALERT_ROUTING_KEY", "alerts.email")


def _publish_alert(payload: dict) -> None:
    credentials = pika.PlainCredentials(RABBITMQ_USER, RABBITMQ_PASS)
    params = pika.ConnectionParameters(
        host=RABBITMQ_HOST,
        port=RABBITMQ_PORT,
        virtual_host=RABBITMQ_VHOST,
        credentials=credentials,
        heartbeat=30,
        blocked_connection_timeout=30,
    )

    connection = pika.BlockingConnection(params)
    channel = connection.channel()

    channel.exchange_declare(exchange=ALERT_EXCHANGE, exchange_type="direct", durable=True)
    channel.queue_declare(queue=ALERT_QUEUE, durable=True)
    channel.queue_bind(queue=ALERT_QUEUE, exchange=ALERT_EXCHANGE, routing_key=ALERT_ROUTING_KEY)

    channel.basic_publish(
        exchange=ALERT_EXCHANGE,
        routing_key=ALERT_ROUTING_KEY,
        body=json.dumps(payload).encode("utf-8"),
        properties=pika.BasicProperties(
            delivery_mode=2,  # persistente
            content_type="application/json",
        ),
    )
    connection.close()


def airflow_task_failure_callback(context: dict) -> None:
    ti = context.get("task_instance")
    dag = context.get("dag")
    exception = context.get("exception")

    payload = {
        "event_type": "airflow_task_failure",
        "timestamp_utc": datetime.now(timezone.utc).isoformat(),
        "dag_id": getattr(dag, "dag_id", None),
        "task_id": getattr(ti, "task_id", None),
        "run_id": context.get("run_id"),
        "try_number": getattr(ti, "try_number", None),
        "log_url": getattr(ti, "log_url", None),
        "exception": str(exception) if exception else None,
    }

    _publish_alert(payload)


def airflow_pipeline_success_callback(**kwargs) -> None:
    payload = {
        "event_type": "airflow_pipeline_success",
        "timestamp_utc": datetime.now(timezone.utc).isoformat(),
        "dag_id": "etl_pipeline",
        "task_id": "alert_on_completion",
        "run_id": kwargs.get("run_id"),
        "try_number": 1,
        "log_url": None,
        "exception": None,
    }
    _publish_alert(payload)