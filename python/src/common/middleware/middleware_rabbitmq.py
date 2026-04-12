import pika
from .middleware import MessageMiddlewareCloseError, MessageMiddlewareDisconnectedError, MessageMiddlewareMessageError, MessageMiddlewareQueue, MessageMiddlewareExchange

class MessageMiddlewareQueueRabbitMQ(MessageMiddlewareQueue):

    def __init__(self, host, queue_name):
        self.host = host
        self.queue_name = queue_name
        self.connection = None
        self.channel = None

        try:
            self.connection = pika.BlockingConnection(
                pika.ConnectionParameters(host=host)
            )
            self.channel = self.connection.channel()
            self.channel.queue_declare(queue=self.queue_name)
            self.channel.basic_qos(prefetch_count=1)
        except pika.exceptions.AMQPConnectionError as e:
            raise MessageMiddlewareDisconnectedError() from e
        except pika.exceptions.AMQPError as e:
            raise MessageMiddlewareMessageError() from e

    def send(self, message):
        try:
            self.channel.basic_publish(
                exchange='',
                routing_key=self.queue_name,
                body=message
            )
        except pika.exceptions.AMQPConnectionError as e:
            raise MessageMiddlewareDisconnectedError() from e
        except pika.exceptions.AMQPError as e:
            raise MessageMiddlewareMessageError() from e

    def start_consuming(self, on_message_callback):
        try:
            def internal_callback(ch, method, properties, body):
                def ack():
                    ch.basic_ack(delivery_tag=method.delivery_tag)

                def nack():
                    ch.basic_nack(delivery_tag=method.delivery_tag, requeue=True)

                on_message_callback(body, ack, nack)

            self.channel.basic_consume(
                queue=self.queue_name,
                on_message_callback=internal_callback,
                auto_ack=False
            )

            self.channel.start_consuming()

        except pika.exceptions.AMQPConnectionError as e:
            raise MessageMiddlewareDisconnectedError() from e
        except pika.exceptions.AMQPError as e:
            raise MessageMiddlewareMessageError() from e

    def stop_consuming(self):
        try:
            if self.channel and self.channel.is_open:
                self.channel.stop_consuming()
        except pika.exceptions.AMQPConnectionError as e:
            raise MessageMiddlewareDisconnectedError() from e
        except Exception:
            pass

    def close(self):
        try:
            if self.channel and self.channel.is_open:
                self.channel.close()

            if self.connection and self.connection.is_open:
                self.connection.close()

        except Exception as e:
            raise MessageMiddlewareCloseError() from e

class MessageMiddlewareExchangeRabbitMQ(MessageMiddlewareExchange):

    def __init__(self, host, exchange_name, routing_keys):
        self.host = host
        self.exchange_name = exchange_name
        self.routing_keys = routing_keys
        self.connection = None
        self.channel = None
        self.queue_name = None

        try:
            self.connection = pika.BlockingConnection(
                pika.ConnectionParameters(host=host)
            )
            self.channel = self.connection.channel()

            self.channel.exchange_declare(
                exchange=self.exchange_name,
                exchange_type='direct'
            )

            result = self.channel.queue_declare(
                queue='',
                exclusive=True
            )
            self.queue_name = result.method.queue

            for routing_key in self.routing_keys:
                self.channel.queue_bind(
                    exchange=self.exchange_name,
                    queue=self.queue_name,
                    routing_key=routing_key
                )

            self.channel.basic_qos(prefetch_count=1)

        except pika.exceptions.AMQPConnectionError as e:
            raise MessageMiddlewareDisconnectedError() from e
        except pika.exceptions.AMQPError as e:
            raise MessageMiddlewareMessageError() from e

    def send(self, message):
        try:
            routing_key = self.routing_keys[0]

            self.channel.basic_publish(
                exchange=self.exchange_name,
                routing_key=routing_key,
                body=message
            )
        except pika.exceptions.AMQPConnectionError as e:
            raise MessageMiddlewareDisconnectedError() from e
        except pika.exceptions.AMQPError as e:
            raise MessageMiddlewareMessageError() from e

    def start_consuming(self, on_message_callback):
        try:
            def internal_callback(ch, method, properties, body):
                def ack():
                    try:
                        ch.basic_ack(delivery_tag=method.delivery_tag)
                    except pika.exceptions.AMQPConnectionError as e:
                        raise MessageMiddlewareDisconnectedError() from e
                    except pika.exceptions.AMQPError as e:
                        raise MessageMiddlewareMessageError() from e

                def nack():
                    try:
                        ch.basic_nack(delivery_tag=method.delivery_tag, requeue=True)
                    except pika.exceptions.AMQPConnectionError as e:
                        raise MessageMiddlewareDisconnectedError() from e
                    except pika.exceptions.AMQPError as e:
                        raise MessageMiddlewareMessageError() from e

                on_message_callback(body, ack, nack)

            self.channel.basic_consume(
                queue=self.queue_name,
                on_message_callback=internal_callback,
                auto_ack=False
            )

            self.channel.start_consuming()

        except pika.exceptions.AMQPConnectionError as e:
            raise MessageMiddlewareDisconnectedError() from e
        except pika.exceptions.AMQPError as e:
            raise MessageMiddlewareMessageError() from e

    def stop_consuming(self):
        try:
            if self.channel and self.channel.is_open:
                self.channel.stop_consuming()
        except pika.exceptions.AMQPConnectionError as e:
            raise MessageMiddlewareDisconnectedError() from e
        except Exception:
            pass

    def close(self):
        try:
            if self.channel and self.channel.is_open:
                self.channel.close()

            if self.connection and self.connection.is_open:
                self.connection.close()

        except Exception as e:
            raise MessageMiddlewareCloseError() from e
