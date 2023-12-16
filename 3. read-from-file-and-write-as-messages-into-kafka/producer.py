from confluent_kafka import Producer
import time
import json

class InvoiceProducer():
    def __init__(self):
        self.topic = "invoices"
        self.conf = {'bootstrap.servers': 'pkc-lzvrd.us-west4.gcp.confluent.cloud:9092',
                     'security.protocol': 'SASL_SSL',
                     'sasl.mechanism': 'PLAIN',
                     'sasl.username': 'XYZ',
                     'sasl.password': 'ABC',
                     'client.id': "vinod-laptop"}

    def delivery_callback(self, err, msg):
        if err:
            print('Message delivery failed with error: ', err)
        else:
            key = msg.key().decode('utf-8') # convert binary data from kafka into human readable string
            invoice_id = json.loads(msg.value().decode('utf-8'))['InvoiceNumber'] # convert binary data from kafka into human readable string, convert it into a json object, extract required field
            offset = msg.offset()
            print('Successfully sent details related to ',invoice_id, ' at offset ',offset)


    def produce_invoices(self, producer):
        cntr = 0
        with open('data/invoices.json') as lines:
            for line in lines:
                invoice = json.loads(line) # convert string to json object
                store_id = invoice['StoreID'] # use json object to extract key for the message
                producer.produce(self.topic, key = store_id, value = line, callback = self.delivery_callback)
                time.sleep(1)
        producer.flush()
        # Without the flush(), i was getting the message - Producer terminating with 5 messages (3805 bytes) still in queue or transit: use flush() to wait for outstanding message delivery
                  

    def start_producer(self):
        print('Producer has started successfully !!!')
        invoice_producer = Producer(self.conf)
        self.produce_invoices(invoice_producer)
        print('Producer has published 5 messages. Closing the producer now')


if __name__ == "__main__":
    invoice_producer = InvoiceProducer()
    invoice_producer.start_producer()