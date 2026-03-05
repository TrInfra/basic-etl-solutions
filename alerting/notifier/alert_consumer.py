import json
import os
import time

import pika

from alerting.producer.email_notifier import EmailNotifier


RABBITMQ_HOST = os.getenv("ALERT_RMQ_HOST", "rabbitmq")
RABBITMQ_PORT = int(os.getenv("ALERT_RMQ_PORT", "5672"))
RABBITMQ_USER = os.getenv("ALERT_RMQ_USER", "admin")
RABBITMQ_PASS = os.getenv("ALERT_RMQ_PASS", "admin")
RABBITMQ_VHOST = os.getenv("ALERT_RMQ_VHOST", "/")

ALERT_EXCHANGE = os.getenv("ALERT_EXCHANGE", "alerts.exchange")
ALERT_QUEUE = os.getenv("ALERT_QUEUE", "alerts.email")
ALERT_ROUTING_KEY = os.getenv("ALERT_ROUTING_KEY", "alerts.email")


def _format_email(payload: dict) -> tuple[str, str]:
    subject = f"[ALERTA AIRFLOW] DAG={payload.get('dag_id')} TASK={payload.get('task_id')}"
    body = (
        f"Evento: {payload.get('event_type')}\n"
        f"Timestamp UTC: {payload.get('timestamp_utc')}\n"
        f"DAG: {payload.get('dag_id')}\n"
        f"Task: {payload.get('task_id')}\n"
        f"Run ID: {payload.get('run_id')}\n"
        f"Tentativa: {payload.get('try_number')}\n"
        f"Log URL: {payload.get('log_url')}\n"
        f"Erro: {payload.get('exception')}\n"
    )
    return subject, body


def run() -> None:
    notifier = EmailNotifier()

    while True:
        try:
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

            def _callback(ch, method, properties, body):
                try:
                    payload = json.loads(body.decode("utf-8"))
                    subject, content = _format_email(payload)
                    notifier.send(subject, content)
                    ch.basic_ack(delivery_tag=method.delivery_tag)
                    print(f"[alerting] alerta processado: {payload.get('dag_id')}::{payload.get('task_id')}")
                except Exception as exc:
                    print(f"[alerting] erro ao processar mensagem: {exc}")
                    ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)

            channel.basic_qos(prefetch_count=10)
            channel.basic_consume(queue=ALERT_QUEUE, on_message_callback=_callback)

            print("[alerting] consumer iniciado...")
            channel.start_consuming()

        except Exception as exc:
            print(f"[alerting] falha conexão/consumo: {exc}. Retry em 5s.")
            time.sleep(5)


if __name__ == "__main__":
    run()