from kafka import KafkaClient, SimpleProducer, SimpleConsumer


def connect_kafka():
    kafka = KafkaClient('localhost:9092')
    print "Connected to kafka"
    return kafka


kafka = connect_kafka()
producer = SimpleProducer(kafka)


def publish(record):
    producer.send_messages("bids", record)

