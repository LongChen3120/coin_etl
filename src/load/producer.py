import kafka


class kafkaProducer():
    broker_ip = "localhost:9092"

    def __init__(self, topic) -> None:
        self.producer = kafka.KafkaProducer(
            bootstrap_servers = self.broker_ip,
            acks = 1
        )
        self.topic = topic

    def send_message(self, message):
        try:
            self.producer.send(self.topic, message.encode("utf-8"))
        except Exception as e:
            print(f"Exception send message:\n{e}")

    def producer_flush(self):
        self.producer.flush()

    def producer_close(self):
        self.producer.close()