# -*- coding:UTF-8 -*-
# UDP 广播接收
from socket import *
import pika
import multiprocessing
from bcFunction import addBlock
import time
import random
import leveldb
import traceback
'''
    6681:交易广播/接收端口
    6682:区块广播/接收端口
    6683:区块验证结果广播发送/接收端口
'''

def tx_recieve():
    '''
    交易接收(来自主节点的广播交易)
    此端口接收的交易为其他节点转发，虽然在其他节点已经验证过，但是仍需投入到requestpool通道重新验证
    :return:
    '''



    try:

        f = open('set.info', 'r')
        set_dic = eval(f.read())

        host_addr = set_dic['addr_udp']
        master_addr = set_dic['master_addr']


        connection = pika.BlockingConnection(
            pika.ConnectionParameters('localhost'))
        channel = connection.channel()
        channel.queue_declare(queue='requestpool')


        credentials = pika.PlainCredentials('admin', 'admin')
        connection_m = pika.BlockingConnection(
            pika.ConnectionParameters(host=master_addr, port=5672, virtual_host='/', credentials=credentials))
        channel_m = connection_m.channel()
        channel_m.exchange_declare(exchange='txp', exchange_type='fanout')

        qn = 'test' + str(random.randint(0, 1000000))+'tx'
        channel_m.queue_declare(queue=qn, exclusive=True)
        channel_m.queue_bind(exchange='txp', queue=qn)



        def callback(ch,method,properties,body):
            channel.basic_publish(
                        exchange='',  # RabbitMQ中所有的消息都要先通过交换机，空字符串表示使用默认的交换机
                        routing_key='requestpool',  # 指定消息要发送到哪个queue
                        body=body.decode('utf-8'))  # 消息的内容



        channel_m.basic_consume(on_message_callback=callback,
                                queue=qn,
                                auto_ack=True

                                )

        channel_m.start_consuming()


    except Exception as e:
        print(e)



def pbftuc():
    '''
    接收主节点需要共识的区块，存入本地消息队列等待验证
    :return:
    '''



    try:

        f = open('set.info', 'r')
        set_dic = eval(f.read())

        host_addr = set_dic['addr_udp']
        master_addr = set_dic['master_addr']


        connection = pika.BlockingConnection(
            pika.ConnectionParameters('localhost'))
        channel = connection.channel()
        channel.queue_declare(queue='pbftuc_local')




        credentials = pika.PlainCredentials('admin', 'admin')
        connection_m = pika.BlockingConnection(
            pika.ConnectionParameters(host=master_addr, port=5672, virtual_host='/', credentials=credentials))
        channel_m = connection_m.channel()
        channel_m.exchange_declare(exchange='pbftuc', exchange_type='fanout')

        qn = 'test' + str(random.randint(0, 1000000))+'tx'
        channel_m.queue_declare(queue=qn, exclusive=True)
        channel_m.queue_bind(exchange='pbftuc', queue=qn)



        def callback(ch,method,properties,body):


            channel.basic_publish(
                        exchange='',  # RabbitMQ中所有的消息都要先通过交换机，空字符串表示使用默认的交换机
                        routing_key='pbftuc_local',  # 指定消息要发送到哪个queue
                        body=body.decode('utf-8'))  # 消息的内容



        channel_m.basic_consume(on_message_callback=callback,
                                queue=qn,
                                auto_ack=True

                                )

        channel_m.start_consuming()


    except Exception as e:

        print(e)







def commit():
    try:


        db = leveldb.LevelDB('./test')
        f = open('set.info', 'r')
        set_dic = eval(f.read())
        noden = set_dic['node_num']
        master_addr = set_dic['master_addr']
        wb=[None,None]
        connection = pika.BlockingConnection(
            pika.ConnectionParameters('localhost'))
        channel = connection.channel()

        channel.queue_declare(queue='pbft')
        lock_v=[False]
        com_dic={}
        sumb=[1]
        timedata=[]

        credentials = pika.PlainCredentials('admin', 'admin')
        connection_m = pika.BlockingConnection(
            pika.ConnectionParameters(host=master_addr, port=5672, virtual_host='/', credentials=credentials))
        channel_m = connection_m.channel()
        channel_m.exchange_declare(exchange='pbftre', exchange_type='fanout')
        qn = 'test' + str(random.randint(0, 1000000))+'cm'
        channel_m.queue_declare(queue=qn,exclusive=True)
        channel_m.queue_bind(queue=qn,exchange='pbftre')

        prenum=[0]
        prenum[0] = int(db.Get(b'blocknum'))
        def callback(ch, method, properties, body):
            bid=body.decode('utf-8')
            #print(bid)
            if bid in com_dic:
                com_dic[bid]+=1
            else:
                com_dic[bid]=1
            #print(com_dic)
            if not lock_v[0]:

                wb[0]=channel.basic_get(queue='pbft',auto_ack=True)[2]

                if wb[0]!=None:
                    wb[0]=wb[0].decode('utf-8')
                    wb[1]=wb[0].split('&')[0]
                    lock_v[0]=True
            if lock_v[0]:
                if wb[1] in com_dic and com_dic[wb[1]]>=2*(noden/3):






                    deallist=eval(wb[0].split('&')[1])
                    dataa=[eval(x.split('$')[2])[1] for x in deallist]
                    mtree=str(addBlock(data=dataa,dataType='hex'))
                    blockdata=str(dataa)

                    print("落块成功：", prenum[0]+1)
                    blockkey='block'+str(prenum[0]+1)
                    mtreekey='mtree'+str(prenum[0]+1)
                    prenum[0]+=1
                    if prenum[0]==8192:

                        print(timedata)
                    db.Put(blockkey.encode('utf-8'),blockdata.encode('utf-8'))
                    db.Put(mtreekey.encode('utf-8'), mtree.encode('utf-8'))





                    kt=time.time()
                    timedata.append(kt)



                    lock_v[0]=False



        channel_m.basic_consume(
            queue=qn,  # 接收指定queue的消息
            on_message_callback=callback,  # 接收到消息后的处理程序

         auto_ack=True
        )  # 指定为True，表示消息接收到后自动给消息发送方回复确认，已收到消息
        # 6. 开始循环等待，一直处于等待接收消息的状态

        channel_m.start_consuming()
    except Exception as e:
        traceback.print_exception()
        print(e)











def startprocess():
    f = open('set.info', 'r')
    set_dic = eval(f.read())
    nodetype = set_dic['node_type']
    if nodetype=='master':
        commit()
        print('主节点通道进程启动')
        #tx_recieve()
    else:
        print('子节点通道进程启动')

        pool = multiprocessing.Pool(processes=3)
        pool.apply_async(tx_recieve)
        pool.apply_async(commit)
        pool.apply_async(pbftuc)
        pool.close()
        pool.join()



if __name__ == '__main__':
    startprocess()


































