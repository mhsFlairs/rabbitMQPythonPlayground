import sys
import json
import pika

EXCHANGE_NAME = "integration_output_exchange"
QUEUE_NAME = "Playground"

HELP_TEXT = """
Usage:
    python rabbitmq_app.py <publisher|consumer> [queue_name]

Arguments:
    publisher   Run in publisher mode to send messages.
    consumer    Run in consumer mode to receive messages.
    queue_name  (Optional) Specify a queue name (default: Playground).

Examples:
    python rabbitmq_app.py publisher
    python rabbitmq_app.py consumer MyQueue

Options:
    --help      Show this help message and exit.
"""

class RabbitMQService:
    def __init__(self, config):
        self.config = config
        self.connection = self.create_connection()
        self.channel = self.create_channel()

    def create_connection(self):
        return pika.BlockingConnection(
            pika.ConnectionParameters(
                host=self.config["host"],
                port=self.config["port"],
                virtual_host=self.config["virtual_host"],
                credentials=pika.PlainCredentials(self.config["user"], self.config["password"]),
            )
        )

    def create_channel(self):
        channel = self.connection.channel()
        channel.exchange_declare(exchange=EXCHANGE_NAME, exchange_type="fanout", durable=True)
        return channel

    def publish_message(self, message):
        self.channel.basic_publish(
            exchange=EXCHANGE_NAME,
            routing_key="",
            body=json.dumps(message),
            properties=pika.BasicProperties(delivery_mode=2)  # Make message persistent
        )
        print("Message published successfully.")

    def consume_messages(self, queue_name):
        self.channel.queue_declare(queue=queue_name, durable=True)
        self.channel.queue_bind(exchange=EXCHANGE_NAME, queue=queue_name)

        def callback(ch, method, properties, body):
            print(f"Received message: {body.decode()}")
            ch.basic_ack(delivery_tag=method.delivery_tag)

        self.channel.basic_qos(prefetch_count=1)
        self.channel.basic_consume(queue=queue_name, on_message_callback=callback)

        print(f"Queue name: {queue_name}. Waiting for messages...")
        self.channel.start_consuming()


if __name__ == '__main__':
    if "--help" in sys.argv:
        print(HELP_TEXT)
        sys.exit(0)

    if len(sys.argv) < 2 or len(sys.argv) > 3:
        print("Invalid arguments.\n" + HELP_TEXT)
        sys.exit(1)

    config = {
        "host": "localhost",
        "user": "guest",
        "password": "guest",
        "port": 5672,
        "virtual_host": "/"
    }

    mode = sys.argv[1].lower()
    queue_name = sys.argv[2] if len(sys.argv) == 3 else QUEUE_NAME

    rabbitmq_service = RabbitMQService(config)

    if mode == "publisher":
        while True:
            message = input("Enter message to publish (or 'e' to exit): ")
            if message.lower() == "e":
                break
            rabbitmq_service.publish_message(message)
    elif mode == "consumer":
        rabbitmq_service.consume_messages(queue_name)
    else:
        print("Invalid mode.\n" + HELP_TEXT)