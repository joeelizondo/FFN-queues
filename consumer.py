"""ConsumerClass"""
from kombu.transport.amqplib import Transport
from kombu.connection import BrokerConnection
from kombu.entity import Queue, Exchange
from kombu.common import eventloop
from kombu import messaging
import logging
import json


class Consumer(object):
    """
    Defines the base Rabbit Consumer class
    """
    def __init__(self, config):
        self.connection_info = {
            'hostname': config['host'],
            'port': int(config['port']),
            'userid': config['username'],
            'password': config['password'],
            'virtual_host': config['vhost']
        }
        self.connection = BrokerConnection(
            transport=Transport,
            **self.connection_info
        )
        self.connection.ensure_connection(self.connection_error)
        self.exchange = Exchange(name=config['exchange'], channel=self.connection)
        if isinstance(config['queue_name'], list):
            self.queues = [Queue(name, exchange=self.exchange) for name in config['queue_name']]
        else:
            self.queues = [Queue(config['queue_name'], exchange=self.exchange)]
        self.callbacks = []
        self.add_callback(self.on_message)
        self.prefetch_count = 1

    def consume(self):
        """Consumes all messages on the queues
        """
        self.callbacks.append(self.ack_message)
        c = messaging.Consumer(self.connection, queues=self.queues, callbacks=self.callbacks, auto_declare=False)
        c.qos(prefetch_count=self.prefetch_count)
        with c:
            for _ in eventloop(self.connection, timeout=10, ignore_timeouts=True):
                pass

    def add_callback(self, callback):
        """Registers a new callback to be called upon message receipt
        Args:
          callback: a function that accepts body, message as args
        """
        def _build_body(body):
            """Helper method to build body"""
            if isinstance(body, dict):
                return body

            try:
                json_data = json.loads(body)
                if "path" not in json_data:
                    json_data["path"] = [None, None]
                return json_data
            except Exception as e:
                logging.error("Invalid JSON. Exception: %s", e)
            return {"invalid_json": str(body), 'path': [None, None]}

        self.callbacks.append(lambda body, message: callback(_build_body(body), message))

    def add_queue(self, queue_name):
        """Adds a queue to the consumer
        Args:
          queue_name: name of queue to be bound to consumer
        """
        self.queues.append(Queue(queue_name, exchange=self.exchange))

    def connection_error(self, exc, interval):
        """Defines action on connection error
        """
        if interval > 10:
            raise IOError("Bad connection to rabbit")
        else:
            logging.error("rabbitmq connection error, retry in %s seconds.  error: %s", interval, exc.strerror if hasattr(exc, 'strerror') else exc)

    def on_message(self, body, _):
        """Defines action to take on receipt of message
        Args:
          body: a decoded message body
          message: the message object
        """
        logging.info("Recieved message %s from one of the following:%s", json.dumps(body), self.queues)

    def ack_message(self, body, message):
        """Defines acking the message after processing
        Args:
          body: a decoded message body
          message: the message object
        """
        logging.info("Acking message %s", json.dumps(body))
        message.ack()
