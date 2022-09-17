import pika

connection = pika.BlockingConnection(
        pika.ConnectionParameters('localhost'))
channel = connection.channel()
channel.queue_declare(queue='pbft')



while True:
        k=channel.basic_get(queue='pbft',auto_ack=True)[2]
        if k:
                print(k)
        else:
                break






while True:
        k=channel.basic_get(queue='checkedR',auto_ack=True)[2]
        if k:
                print(k)
        else:
                break


try:
        while True:
                k=channel.basic_get(queue='requestpool',auto_ack=True)[2]
                if k:
                        print(k)
                else:
                        break
except:
        print('rp')

try:
        while True:
                k=channel.basic_get(queue='pbftuc_local',auto_ack=True)[2]
                if k:
                        print('zz',k)
                else:
                        break
                        #requestpool
except:
        print('uc-l')

try:
        while True:
                k=channel.basic_get(queue='pbftuc',auto_ack=True)[2]
                if k:
                        print(k)
                else:
                        break
except:
        print('uc')

try:
        while True:
                k=channel.basic_get(queue='pbftre',auto_ack=True)[2]
                if k:
                        print(k)
                else:
                        break
except:
        print('pre')
