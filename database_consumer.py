# Use this file to setup the database consumer that stores the ride information in the database

import imp
import json
import pika
from pymongo import MongoClient
import time

time.sleep(10)

client = MongoClient()

# client = MongoClient("mongodb://localhost:27017/")
# client = MongoClient("mongo:27017/")
# client = MongoClient("mongodb:mongodb//mongo:27017/")
client = MongoClient("mongodb://mongodb")

# Access database
mydatabase = client['database']

# Access collection of the database
mycollection = mydatabase["ridetable"]

def main():
    connection = pika.BlockingConnection(pika.ConnectionParameters(host="rabbitmq"))
    channel = connection.channel()
    channel.queue_declare(queue='database')

    def callback(ch, method, properties, body):
        ride = json.loads(body.decode('utf-8'))
        sleeptime = ride['time']
        print("*******************************************")
        rec = mydatabase.myTable.insert_one(ride)
        for i in mydatabase.myTable.find():
            print(i)
        print(" [x] Received ", sleeptime)

    channel.basic_consume(queue = 'database', on_message_callback = callback, auto_ack = True)

    print(' [*] Waiting for messages. To exit press CTRL+C')
    channel.start_consuming()

if __name__ == '__main__':
    main()