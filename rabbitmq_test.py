import pika

connection = pika.BlockingConnection(
    pika.ConnectionParameters("localhost", 5672)
)
channel = connection.channel()

channel.queue_declare(queue="university-test", durable=False)
message = "Hello from RabbitMQ in my Distributed Systems project!"

channel.basic_publish(
    exchange="",
    routing_key="university-test",
    body=message,
)

method, properties, body = channel.basic_get(
    queue="university-test", auto_ack=True
)

if method:
    print("RabbitMQ message received:", body.decode())
else:
    print("No message received.")

connection.close()
