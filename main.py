# coding: utf-8
# from gevent import monkey
# monkey.patch_all()
from time import  time

import gevent
from flask import Flask, request
import random
import pika
from gevent.pywsgi import WSGIServer

app = Flask(__name__)

@app.route('/add', methods=['GET', 'POST'])
def add():
    connection = pika.BlockingConnection(
        pika.ConnectionParameters('localhost'))
    channel = connection.channel()
    channel.queue_declare(queue='requestpool')

    method_args = str(request.args.get('args'))
    timeV = time()
    key = str(timeV).replace('.', '') + str(random.random()).replace('.', '')



    channel.basic_publish(
        exchange='',  # RabbitMQ中所有的消息都要先通过交换机，空字符串表示使用默认的交换机
        routing_key='requestpool',  # 指定消息要发送到哪个queue
        body=str(key+'$'+str(timeV)+'$'+str(method_args)))  # 消息的内容




    '''
    (method_args,timeV,key)放入未验证的临时请求池
    (key,'')放入信息查看池
    '''
    return 'Suc'




if __name__ == "__main__":
    WSGIServer(('0.0.0.0', 8000), app).serve_forever()



