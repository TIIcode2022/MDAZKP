import pika
from time import time,sleep
import random

connection = pika.BlockingConnection(
        pika.ConnectionParameters('localhost'))
channel = connection.channel()
channel.queue_declare(queue='requestpool')
f=open('proof.json','r')
testc='db282902b8caf1b9fe5f7050a11cb2cb1f99266b6f58dd9f40159d468cfa5e36'
asset_hashid='ca978112ca1bbdcafac231b39a23dc4da786eff8147c4e72b9807785afee48bb'
args3=f.read()

outt=[]
z=0
sss=time()
for i in range(8200):
    timeV = time()
    outt.append(timeV)
    key = str(timeV).replace('.', '') + str(random.randint(1000000,9999999))
    channel.basic_publish(
        exchange='',  # RabbitMQ中所有的消息都要先通过交换机，空字符串表示使用默认的交换机
        routing_key='requestpool',  # 指定消息要发送到哪个queue
        body=str(key + '$' + str(timeV) + '$' + str(['GE',testc,args3,asset_hashid])))  # 消息的内容
print(time()-sss)
f=open('tw8b1.txt', 'w')
f.write(str(outt))
f.close()



