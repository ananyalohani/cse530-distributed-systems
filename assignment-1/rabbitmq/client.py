import pika
import json
import uuid
import sys
import inquirer
from inquirer.themes import BlueComposure
from colorama import Fore


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
    client = Client(client_name)
    while True:
        q_method = inquirer.List(name='method', message='Choose an operation', choices=[
                                 "GetServerList", "JoinServer", "LeaveServer", "PublishArticle", "GetArticles", "Exit"])
        method = inquirer.prompt([q_method], theme=BlueComposure())['method']

        if method == "Exit":
            exit()

        params = None
        if method == 'GetServerList':
            response = client.get_server_list(registry_name)
            print(Fore.GREEN, response.decode(), end='\n\n')
        else:
            q_server_id = inquirer.Text(
                name='server_id', message='Enter server id', validate=lambda _, x: x != '')
            server_id = inquirer.prompt(
                [q_server_id], theme=BlueComposure())['server_id']

            if method == 'PublishArticle':
                q_article = [
                    inquirer.List(name='type', message='Enter article type', choices=[
                                  'SPORTS', 'FASHION', 'POLITICS']),
                    inquirer.Text(
                        name='author', message='Enter author name', validate=lambda _, x: x != ''),
                    inquirer.Text(name='content',
                                  message='Enter article content', validate=lambda _, x: x != ''),
                ]
                answers = inquirer.prompt(q_article, theme=BlueComposure())
                article_type = answers['type']
                author = answers['author'].strip()
                content = answers['content'].strip()
                content = content[:min(len(content), 200)]
                params = json.dumps({
                    'type': article_type,
                    'author': author,
                    'content': content
                })
            elif method == 'GetArticles':
                q_article = [
                    inquirer.List(name='type', message='Enter article type', choices=[
                                  'SPORTS', 'FASHION', 'POLITICS']),
                    inquirer.Text(
                        name='author', message='Enter author name', validate=lambda _, x: x != ''),
                    inquirer.Text(
                        name='date', message='Enter date (DD/MM/YYYY)', validate=lambda _, x: x != ''),
                ]
                answers = inquirer.prompt(q_article, theme=BlueComposure())
                article_type = answers['type']
                author = answers['author']
                date = answers['date']
                if not article_type or not author or not date:
                    print('Invalid params!')
                elif date == '_' or (article_type == '_' and author == '_'):
                    print('Invalid params!')
                params = json.dumps({
                    'type': article_type if article_type != '_' else None,
                    'author': author if author != '_' else None,
                    'date': date
                })

            response = client.call(f'{server_id}_server_rpc', method, params)
            # print(response)
            if 'FAILURE' in response.decode():
                print(Fore.RED, response.decode(), end='\n\n')
            else:
                print(Fore.GREEN, response.decode(), end='\n\n')
