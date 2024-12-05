import pika
import datetime
import time

# Generic functions needed to work with rmq


# Connect to rmq using pika library
def connect(host, port, user, password):
    cred = pika.credentials.PlainCredentials(user, password)
    parameters = pika.ConnectionParameters(host=host, port=port, credentials=cred, heartbeat=0)
    return pika.BlockingConnection(parameters)


# Publish msg to exchange or queue
def publish(conn, exchange, route, message, headers=None, callback_queue=None, corr_id=None):
    if conn.is_closed:
        return

    properties = pika.BasicProperties(delivery_mode=2, headers=headers, reply_to=callback_queue, correlation_id=corr_id)
    publish_channel = conn.channel()
    publish_channel.basic_publish(exchange=exchange, routing_key=route, body=message, properties=properties)
    publish_channel.close()


# Send request to database queue using a temporary queue
def db_request(conn, exchange, routing_key, message):
    channel = conn.channel()
    private_queue = channel.queue_declare(exclusive=True).method.queue

    publish(conn, exchange, routing_key, message, callback_queue=private_queue)

    start = datetime.datetime.now()
    # Loop until information is found in private_queue
    while channel.is_open:
        time.sleep(0.1)
        ok, _, body = channel.basic_get(private_queue, no_ack=True)
        dt = (datetime.datetime.now() - start).total_seconds()
        if ok or dt > 5: # Quit if no answer after 5 seconds
            channel.queue_delete(private_queue)
            channel.close()
    return body
