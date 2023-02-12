import pika
import json


class Registry(object):

    def __init__(self, name):
        self.MAX_SERVERS = 10
        self.connection = pika.BlockingConnection(
            pika.ConnectionParameters(host='localhost'))
        self.channel = self.connection.channel()
        self.rpc_queue = self.channel.queue_declare(
            queue=f'{name}_registry_rpc')
        self.servers = []

    def on_register(self, ch, method, props, server_id):
        if len(self.servers) > self.MAX_SERVERS:
            print(f" [.] MAX_SERVERS exceeded, rejecting server {server_id}")
            ch.basic_publish(exchange='', routing_key=props.reply_to, properties=pika.BasicProperties(
                correlation_id=props.correlation_id), body='FAILURE')
            ch.basic_ack(delivery_tag=method.delivery_tag)
            return

        if server_id in self.servers:
            print(f" [.] Server {server_id} already registered")
            ch.basic_publish(exchange='', routing_key=props.reply_to, properties=pika.BasicProperties(
                correlation_id=props.correlation_id), body='SUCCESS')
            ch.basic_ack(delivery_tag=method.delivery_tag)
            return

        print(f" [.] Registering server {server_id}")
        self.servers.append(server_id)
        ch.basic_publish(exchange='', routing_key=props.reply_to, properties=pika.BasicProperties(
            correlation_id=props.correlation_id), body='SUCCESS')

        ch.basic_ack(delivery_tag=method.delivery_tag)

    def on_request(self, ch, method, props, body):
        payload = json.loads(body)
        if payload['method'] == "Register":
            self.on_register(ch, method, props, payload['params']['server_id'])
        elif payload['method'] == "GetServerList":
            print(f" [.] Returning server list to {payload['client_id']}")
            ch.basic_publish(exchange='', routing_key=props.reply_to, properties=pika.BasicProperties(
                correlation_id=props.correlation_id), body=json.dumps(self.servers))
            ch.basic_ack(delivery_tag=method.delivery_tag)

    def start(self):
        self.channel.basic_qos(prefetch_count=1)
        self.channel.basic_consume(queue=self.rpc_queue.method.queue,
                                   on_message_callback=self.on_request)
        print(" [x] Awaiting RPC requests")
        self.channel.start_consuming()


if __name__ == "__main__":
    registry = Registry("r1")
    registry.start()
