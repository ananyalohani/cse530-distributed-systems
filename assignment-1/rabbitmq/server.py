import pika
import uuid
import json
import time
import datetime
import sys

class Server(object):

    def __init__(self, server_id=None):
        self.MAX_CLIENTS = 5

        self.connection = pika.BlockingConnection(
            pika.ConnectionParameters('localhost'))
        self.channel = self.connection.channel()

        response = self.channel.queue_declare(queue='', exclusive=True)
        self.callback_queue = response.method.queue

        self.channel.basic_consume(
            queue=self.callback_queue, on_message_callback=self.on_response)

        self.server_id = server_id or uuid.uuid4()
        self.response = None
        self.correlation_id = None
        self.clientele = []
        self.articles = []

    def on_response(self, ch, method, props, body):
        if self.correlation_id == props.correlation_id:
            self.response = body

    def register(self, registry_name):
        self.response = None
        self.correlation_id = str(uuid.uuid4())
        self.channel.basic_publish(exchange='', routing_key=f'{registry_name}_registry_rpc',
                                   properties=pika.BasicProperties(reply_to=self.callback_queue, correlation_id=self.correlation_id), body=json.dumps({ 'method': 'Register', 'params': { 'server_id': f'{self.server_id}' } }))
        self.connection.process_data_events(time_limit=None)
        return self.response

    def start(self):
        self.channel.queue_declare(queue=f'{self.server_id}_server_rpc')
        self.channel.basic_consume(
            queue=f'{self.server_id}_server_rpc', on_message_callback=self.on_request)
        print(" [x] Awaiting client requests")
        self.channel.start_consuming()

    def on_request(self, ch, method, props, body):
        payload = json.loads(body)

        print(f" [.] {payload['method']} request from {payload['client_id']}")
        if payload['method'] == 'JoinServer':
            if len(self.clientele) < self.MAX_CLIENTS:
                self.clientele.append(payload['client_id'])
                ch.basic_publish(exchange='', routing_key=props.reply_to,
                                 properties=pika.BasicProperties(correlation_id=props.correlation_id), body="SUCCESS")
            if payload['client_id'] in self.clientele:
                ch.basic_publish(exchange='', routing_key=props.reply_to,
                                 properties=pika.BasicProperties(correlation_id=props.correlation_id), body="SUCCESS")
            else:
                ch.basic_publish(exchange='', routing_key=props.reply_to,
                                 properties=pika.BasicProperties(correlation_id=props.correlation_id), body="FAILURE")
            ch.basic_ack(delivery_tag=method.delivery_tag)

        elif payload['method'] == 'LeaveServer':
            if payload['client_id'] in self.clientele:
                self.clientele.remove(payload['client_id'])
                ch.basic_publish(exchange='', routing_key=props.reply_to,
                                 properties=pika.BasicProperties(correlation_id=props.correlation_id), body="SUCCESS")
            else:
                ch.basic_publish(exchange='', routing_key=props.reply_to,
                                 properties=pika.BasicProperties(correlation_id=props.correlation_id), body="FAILURE")
            ch.basic_ack(delivery_tag=method.delivery_tag)

        elif payload['method'] == 'PublishArticle':
            if payload['client_id'] in self.clientele:
                article = json.loads(payload['params'])
                # print(article['type'])
                article['time'] = time.time()
                self.articles.append(article)
                ch.basic_publish(exchange='', routing_key=props.reply_to,
                                 properties=pika.BasicProperties(correlation_id=props.correlation_id), body="SUCCESS")
            else:
                ch.basic_publish(exchange='', routing_key=props.reply_to,
                                 properties=pika.BasicProperties(correlation_id=props.correlation_id), body="FAILURE")
            ch.basic_ack(delivery_tag=method.delivery_tag)

        elif payload['method'] == 'GetArticles':
            request = json.loads(payload['params'])
            date = request['date']
            timestamp = time.mktime(datetime.datetime.strptime(date,
                                    "%d/%m/%Y").timetuple())
            result = filter(lambda x: x['time'] > timestamp, self.articles)
            result = filter(lambda x: x['type'] == request['type'], result) if request['type'] != None else result
            result = filter(lambda x: x['author'] == request['author'], result) if request['author'] != None else result
            ch.basic_publish(exchange='', routing_key=props.reply_to,
                                properties=pika.BasicProperties(correlation_id=props.correlation_id), body=json.dumps(list(result)))
            ch.basic_ack(delivery_tag=method.delivery_tag)

        else:
            ch.basic_publish(exchange='', routing_key=props.reply_to,
                             properties=pika.BasicProperties(correlation_id=props.correlation_id), body="INVALID REQUEST")
            ch.basic_ack(delivery_tag=method.delivery_tag)

if __name__ == '__main__':
    server_id = sys.argv[1] if len(sys.argv) > 1 else "SERVER1"
    server = Server(server_id)
    print(" [x] Requesting server registration")
    response = server.register("r1")
    print(" [.] Got %r" % response)
    if response.decode() == "SUCCESS":
        server.start()
