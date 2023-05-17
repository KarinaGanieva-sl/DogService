import json
import redis
import threading

rc = redis.Redis(
    host='redis-17072.c285.us-west-2-2.ec2.cloud.redislabs.com',
    port=17072,
    password='4PLTzbkfHThICHNiBqoL2PGnljfBK8bh')

databus_channel = 'dogservice_chat'
chat_messages_key = 'dogservice_chat_messages'

MESSAGE_TYPE_CHAT = 'chat'
MESSAGE_TYPE_JOIN = 'join'
MESSAGE_TYPE_LEAVE = 'leave'


def populate_redis():
    for i in range(1, 20):
        chat_message = f'[user {i}]: hello'
        rc.lpush(chat_messages_key, chat_message)


def receive_messages():
    pubsub = rc.pubsub()
    pubsub.subscribe(databus_channel)
    for message in pubsub.listen():
        if message['type'] == 'message':
            process_message(message['data'])


def process_message(data):
    message = json.loads(data.decode())
    message_type = message['type']
    sender = message['sender']
    content = message['content']

    if message_type == MESSAGE_TYPE_CHAT:
        chat_message = f'[{sender}]: {content}'
        rc.lpush(chat_messages_key, chat_message)
        print(chat_message)
    elif message_type == MESSAGE_TYPE_JOIN:
        print(f'{sender} joined the chat')
    elif message_type == MESSAGE_TYPE_LEAVE:
        print(f'{sender} left the chat')


def send_message(message_type, sender, content):
    message = json.dumps({
        'type': message_type,
        'sender': sender,
        'content': content
    })
    rc.publish(databus_channel, message)


def chat_input():
    while True:
        message = input()
        send_message(MESSAGE_TYPE_CHAT, 'user1', message)


def retrieve_chat_messages():
    chat_messages = rc.lrange(chat_messages_key, 0, -1)
    print("Chat Messages:")
    for chat_message in chat_messages:
        print(chat_message.decode())


populate_redis()
receive_thread = threading.Thread(target=receive_messages)
receive_thread.daemon = True
receive_thread.start()

retrieve_chat_messages()

chat_input()
