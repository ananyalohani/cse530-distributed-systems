import pika
import json
import uuid
import sys


class Client(object):

    def __init__(self, name):
        self.client_id = name or uuid.uuid4()

        self.connection = pika.BlockingConnection(
            pika.ConnectionParameters('localhost'))
        self.channel = self.connection.channel()

        response = self.channel.queue_declare(
            queue='', exclusive=True)
        self.callback_queue = response.method.queue

        self.channel.basic_consume(
            queue=self.callback_queue, on_message_callback=self.on_response, auto_ack=True)

        self.response = None
        self.correlation_id = None

    def on_response(self, ch, method, props, body):
        if self.correlation_id == props.correlation_id:
            self.response = body

    def call(self, routing_key, method, params=None):
        if method not in ['GetServerList', 'JoinServer', 'LeaveServer', 'PublishArticle', 'GetArticles']:
            print('Invalid method!')
            return

        self.response = None
        self.correlation_id = str(uuid.uuid4())
        self.channel.basic_publish(exchange='', routing_key=routing_key,
                                   properties=pika.BasicProperties(
                                       reply_to=self.callback_queue, correlation_id=self.correlation_id),
                                   body=json.dumps({'method': method, 'params': params, 'client_id': self.client_id}))
        self.connection.process_data_events(time_limit=5000)
        return self.response

    def get_server_list(self, registry_name):
        return self.call(f'{registry_name}_registry_rpc', 'GetServerList')


if __name__ == "__main__":
    registry_name = 'r1'
    client_name = sys.argv[1]
    method = sys.argv[2] if len(sys.argv) > 2 else None
    server_id = sys.argv[3] if len(sys.argv) > 3 else None
    client = Client(client_name)
    params = None
    if method == 'GetServerList':
        response = client.get_server_list(registry_name)
        print(response.decode())
    else:
        if method == 'PublishArticle':
            article_type = sys.argv[4] if len(sys.argv) > 4 else None
            author = sys.argv[5] if len(sys.argv) > 5 else None
            content = sys.argv[6] if len(sys.argv) > 6 else None
            if not article_type or not author or not content:
                print('Invalid params!')
                exit()
            params = json.dumps({
                'type': article_type,
                'author': author,
                'content': content
            })
        elif method == 'GetArticles':
            article_type = sys.argv[4] if len(sys.argv) > 4 else None
            author = sys.argv[5] if len(sys.argv) > 5 else None
            date = sys.argv[6] if len(sys.argv) > 6 else None
            if not article_type or not author or not date:
                print('Invalid params!')
                exit()
            elif date == '_' or (article_type == '_' and author == '_'):
                print('Invalid params!')
                exit()
            params = json.dumps({
                'type': article_type if article_type != '_' else None,
                'author': author if author != '_' else None,
                'date': date
            })

        response = client.call(f'{server_id}_server_rpc', method, params)
        print(response.decode())

# CLIENT REQUEST FORMATS:
# python3 client.py ananya GetServerList
# python3 client.py ananya JoinServer SERVER1
# python3 client.py ananya LeaveServer SERVER1
# python3 client.py ananya PublishArticle SERVER1 SPORTS "Ananya Lohani" "Lorem Ipsum"
# python3 client.py ananya GetArticles SERVER1 SPORTS "Ananya Lohani" "10/01/2022"
# python3 client.py ananya GetArticles SERVER1 SPORTS _ "10/01/2021"
# python3 client.py ananya GetArticles SERVER1 _ "Ananya Lohani" "10/01/2021"
