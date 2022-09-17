import pika
import multiprocessing
import time
import traceback
import random

import defaultMethod
from defaultMethod import method
from selfMethod import selfMethod
from socket import *


# 接受交易请求，并使用selfMethod验证，然后发送给队列checkedR

def worker(worker):
    '''

    :param worker: 编号，用于区分进程以适配不同的资源
    :return:
    '''
    try:
        print(worker, 'start')

        #  创建一个到RabbitMQ server的连接，如果连接的不是本机，
        # 则在pika.ConnectionParameters中传入具体的ip和port即可
        connection = pika.BlockingConnection(
            pika.ConnectionParameters('localhost'))

        channel = connection.channel()

        channel.queue_declare(queue='requestpool')

        # 4. 定义消息处理程序

        channel_c = connection.channel()
        channel_c.queue_declare(queue='checkedR')

        def callback(ch, method, properties, body):
            content = body.decode('utf-8')
            content_list = content.split('$')
            args = eval(content_list[2])



            if selfMethod(args, worker):
                #print('jc')
                channel_c.basic_publish(
                        exchange='',  # RabbitMQ中所有的消息都要先通过交换机，空字符串表示使用默认的交换机
                        routing_key='checkedR',  # 指定消息要发送到哪个queue
                        body=body.decode('utf-8'))
            else:
                print('False')


            ch.basic_ack(delivery_tag=method.delivery_tag)

            # 5. 接收来自指定queue的消息

        channel.basic_qos(prefetch_count=1)
        channel.basic_consume(
            queue='requestpool',  # 接收指定queue的消息
            on_message_callback=callback,  # 接收到消息后的处理程序

        )  # 指定为True，表示消息接收到后自动给消息发送方回复确认，已收到消息
        # 6. 开始循环等待，一直处于等待接收消息的状态

        channel.start_consuming()
    except Exception as e:
        print('w',e)


def recieve_master(assetRecord):
    '''
    主节点程序
    接收通过验证的订单到txpool，并将交易广播给其他节点（ADDR） 如果txpool订单数目超过blocksize的大小，则出块
    ,并广播给pbtfuc管道等待共识


    :return:
    '''





    try:
        newrecordnum=[0]
        t = [0, 0, 0]
        txpool = []
        f = open('set.info', 'r')
        set_dic = eval(f.read())
        blocksize = set_dic['blocksize']
        host_addr = set_dic['addr_udp']
        # 1. 创建一个到RabbitMQ server的连接，如果连接的不是本机，
        # 则在pika.ConnectionParameters中传入具体的ip和port即可
        connection = pika.BlockingConnection(
            pika.ConnectionParameters('localhost'))
        # 2. 创建一个channel
        channel = connection.channel()
        # 3. 创建队列，queue_declare可以使用任意次数，
        # 如果指定的queue不存在，则会创建一个queue，如果已经存在，
        # 则不会做其他动作，官方推荐，每次使用时都可以加上这句
        channel.queue_declare(queue='checkedR')



        channel_pbft = connection.channel()
        channel_pbft.queue_declare(queue='pbft')



        channel_txp = connection.channel()
        channel_txp.exchange_declare(exchange='txp', exchange_type='fanout')

        channel_pbftuc = connection.channel()
        channel_pbftuc.exchange_declare(exchange='pbftuc', exchange_type='fanout')

        channel_pbftre = connection.channel()
        channel_pbftre.exchange_declare(exchange='pbftre', exchange_type='fanout')


        def callback(ch, method, properties, body):

            t[1]+=1

            content = body.decode('utf-8')

            channel_txp.basic_publish(
                exchange='txp',
                routing_key='',
                body=content
            )

            content_list = content.split('$')
            args = eval(content_list[2])

            #check if asset apear in record
            if args[0]=='GE':
                if args[3] not in assetRecord:
                    assetRecord[args]=content_list[0]
                    txpool.append(content)
                    newrecordnum[0]+=1
                    #every 10000 record store them to disk
                    if newrecordnum[0]>10000:
                        f=open('AssetRecord.txt','w')
                        f.write(str(assetRecord))
                        f.close()
                        newrecordnum[0]=0

            else:
                txpool.append(content)



            if len(txpool) >= blocksize:

                block_id = str(txpool[0].split('$')[0])
                block = str(block_id) + '&' + str(txpool[:blocksize])

                del txpool[:blocksize]


                channel_pbft.basic_publish(
                    exchange='',
                    routing_key='pbft',  # 指定消息要发送到哪个queue
                    body=block
                )

                channel_pbftuc.basic_publish(
                    exchange='pbftuc',
                    routing_key='',  # 指定消息要发送到哪个queue
                    body=block
                )

                channel_pbftre.basic_publish(
                    exchange='pbftre',
                    routing_key='',  # 指定消息要发送到哪个queue
                    body=block_id
                )



                t[0]+=1

                print('主节点区块生成：',t[0])


            ch.basic_ack(delivery_tag=method.delivery_tag)

        # 5. 接收来自指定queue的消息

        channel.basic_consume(
            queue='checkedR',  # 接收指定queue的消息
            on_message_callback=callback,  # 接收到消息后的处理程序

            # auto_ack=True
        )  # 指定为True，表示消息接收到后自动给消息发送方回复确认，已收到消息
        # 6. 开始循环等待，一直处于等待接收消息的状态
        channel.basic_qos(prefetch_count=1)
        channel.start_consuming()
    except Exception as e:
        print('m',e)




def recieve_servant(assetRecord):
    '''

    子节点tx处理程序
    :return:
    '''




    try:
        t = [0, 0, 0]
        js=[0,None]
        lock_vv=[False]
        txpool = []
        f = open('set.info', 'r')
        set_dic = eval(f.read())

        newrecordnum= [0]


        host_addr = set_dic['addr_udp']
        master_addr=set_dic['master_addr']
        ns=set_dic['blocksize']
        # 1. 创建一个到RabbitMQ server的连接，如果连接的不是本机，
        # 则在pika.ConnectionParameters中传入具体的ip和port即可
        connection = pika.BlockingConnection(
            pika.ConnectionParameters('localhost'))
        # 2. 创建一个channel
        channel = connection.channel()
        # 3. 创建队列，queue_declare可以使用任意次数，
        # 如果指定的queue不存在，则会创建一个queue，如果已经存在，
        # 则不会做其他动作，官方推荐，每次使用时都可以加上这句
        channel.queue_declare(queue='checkedR')
        channel.queue_declare(queue='pbftuc_local')

        credentials=pika.PlainCredentials('admin','admin')
        connection_m=pika.BlockingConnection(pika.ConnectionParameters(host=master_addr,port=5672,virtual_host='/',credentials=credentials))
        channel_m=connection_m.channel()



        channel_pbftre = connection_m.channel()
        channel_pbftre.exchange_declare(exchange='pbftre', exchange_type='fanout')


        channel_pbft = connection.channel()

        channel_pbft.queue_declare(queue='pbft')

        def callback(ch, method, properties, body):

            content = body.decode('utf-8')

            content_list = content.split('$')
            args = eval(content_list[2])

            # check if asset apear in record
            if args[0] == 'GE':
                if args[3] not in assetRecord:
                    assetRecord[args] = content_list[0]
                    txpool.append(content)
                    newrecordnum[0] += 1
                    # every 10000 record store them to disk
                    if newrecordnum[0] > 10000:
                        f = open('AssetRecord.txt', 'w')
                        f.write(str(assetRecord))
                        f.close()
                        newrecordnum[0] = 0
            else:
                txpool.append(content)


            js[0]+=1


            if True:
                if not lock_vv[0]:
                    js[1]=channel.basic_get(queue='pbftuc_local',auto_ack=True)[2]
                    if js[1] != None:
                        lock_vv[0]=True
                if lock_vv[0]:
                    if js[1]!=None and js[0]>=ns:


                        js[1]=js[1].decode('utf-8')
                        blockcontentlist=eval(js[1].split('&')[1])
                        blockid=js[1].split('&')[0]

                        tag=True

                        chaji=list((set(txpool)^set(blockcontentlist))-set(txpool))

                        for x in chaji:
                            x_list = x.split('$')
                            args = eval(x_list[2])
                            if selfMethod(args, -1):
                                continue
                            else:
                                tag = False
                                break
                        js[0]=0

                        lock_vv[0]=False


                        # for x in blockcontentlist:
                        #
                        #     if x in txpool:
                        #         txpool.remove(x)
                        #         continue
                        #     else:
                        #         x_list = x.split('$')
                        #         args = eval(x_list[2])
                        #         if selfMethod(args,-1):
                        #             continue
                        #         else:
                        #             tag=False
                        #             break

                        if tag:




                            channel_pbft.basic_publish(
                                exchange='',
                                routing_key='pbft',  # 指定消息要发送到哪个queue
                                body=js[1]
                            )

                            channel_pbftre.basic_publish(
                                exchange='pbftre',
                                routing_key='',  # 指定消息要发送到哪个queue
                                body=blockid
                            )
                        js[1] = None

            ch.basic_ack(delivery_tag=method.delivery_tag)


        # 5. 接收来自指定queue的消息

        channel.basic_consume(
            queue='checkedR',  # 接收指定queue的消息
            on_message_callback=callback,  # 接收到消息后的处理程序

            # auto_ack=True
        )  # 指定为True，表示消息接收到后自动给消息发送方回复确认，已收到消息
        # 6. 开始循环等待，一直处于等待接收消息的状态
        channel.basic_qos(prefetch_count=1)
        channel.start_consuming()
    except:
        traceback.print_exc()








def start_process():
    f = open('set.info', 'r')
    set_dic = eval(f.read())

    try:
        f=open('AssetRecord.txt','r')
        assetRecord=eval(f.read())
    except:
        print('Record file wrong')
        exit(-1)

    if set_dic['node_type'] == 'master':
        pool = multiprocessing.Pool(processes=set_dic['workersNum'] + 1)
        pool.apply_async(recieve_master,args=(assetRecord,))
        for i in range(set_dic['workersNum']):
            pool.apply_async(worker, args=(i,))
    else:
        pool = multiprocessing.Pool(processes=set_dic['workersNum']+1)
        pool.apply_async(recieve_servant,args=(assetRecord,))
        for i in range(set_dic['workersNum']):
            pool.apply_async(worker, args=(i,))

    pool.close()
    pool.join()


if __name__ == '__main__':
    start_process()
    #recieve_master()
