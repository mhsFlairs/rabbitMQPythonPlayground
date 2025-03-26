import sys
import json
import pika

HELP_TEXT = """
Usage:
    python main.py <publisher|consumer> <exchange_name> [queue_name]

Arguments:
    publisher     Run in publisher mode to send messages.
    consumer      Run in consumer mode to receive messages.
    exchange_name The name of the exchange where messages should be published.
    queue_name    (Optional) Specify a queue name (default: Playground).

Examples:
    python main.py publisher my_exchange
    python main.py consumer my_exchange MyQueue

Options:
    --help        Show this help message and exit.
"""

DEFAULT_QUEUE_NAME = "Playground"


class RabbitMQService:
    def __init__(self, config, exchange_name):
        self.config = config
        self.exchange_name = exchange_name
        self.connection = self.create_connection()
        self.channel = self.create_channel()

    def create_connection(self):
        return pika.BlockingConnection(
            pika.ConnectionParameters(
                host=self.config["host"],
                port=self.config["port"],
                virtual_host=self.config["virtual_host"],
                credentials=pika.PlainCredentials(
                    self.config["user"], self.config["password"]
                ),
            )
        )

    def create_channel(self):
        channel = self.connection.channel()
        channel.exchange_declare(
            exchange=self.exchange_name, exchange_type="fanout", durable=True
        )
        return channel

    def publish_message(self, message):
        self.channel.basic_publish(
            exchange=self.exchange_name,
            routing_key="",
            body=json.dumps(message),
            properties=pika.BasicProperties(delivery_mode=2),  # Persistent message
        )
        print("Message published successfully.")

    def consume_messages(self, queue_name):
        self.channel.queue_declare(queue=queue_name, durable=True)
        self.channel.queue_bind(exchange=self.exchange_name, queue=queue_name)

        def callback(ch, method, properties, body):
            print(f"Received message: {body.decode()}")
            ch.basic_ack(delivery_tag=method.delivery_tag)

        self.channel.basic_qos(prefetch_count=1)
        self.channel.basic_consume(queue=queue_name, on_message_callback=callback)

        print(f"Queue name: {queue_name}. Waiting for messages...")
        self.channel.start_consuming()


if __name__ == "__main__":
    if "--help" in sys.argv:
        print(HELP_TEXT)
        sys.exit(0)

    if len(sys.argv) < 3 or len(sys.argv) > 4:
        print("Invalid arguments.\n" + HELP_TEXT)
        sys.exit(1)

    mode = sys.argv[1].lower()
    exchange_name = sys.argv[2]
    queue_name = sys.argv[3] if len(sys.argv) == 4 else DEFAULT_QUEUE_NAME

    config = {
        "host": "localhost",
        "user": "guest",
        "password": "guest",
        "port": 5672,
        "virtual_host": "/",
    }

    rabbitmq_service = RabbitMQService(config, exchange_name)

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
