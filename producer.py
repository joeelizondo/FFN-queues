"""ProducerClass"""
from kombu.transport.amqplib import Transport
from kombu.connection import BrokerConnection
from kombu.entity import Exchange
from kombu import messaging
import logging
import json


class Producer(object):
    """ Defines the base Rabbit Producer class
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
        self.route = config.get('route')
        self._producer = messaging.Producer(self.connection, exchange=self.exchange, routing_key=self.route, auto_declare=False)

    def connection_error(self, exc, interval):
        """Defines action on connection error
        """
        if interval > 10:
            raise IOError("Bad connection to rabbit")
        else:
            logging.error("rabbitmq connection error, retry in %s seconds.  error: %s", interval, exc.strerror if hasattr(exc, 'strerror') else exc)

    def publish(self, payload, route=None):
        """Defines the publish action
        Args:
          payload: a message payload
          route: a message route
        """
        if not route:
            route = self.route
        logging.info("Published %s to %s with a route of %s", json.dumps(payload), self.exchange, route)
        return self._producer.publish(payload, route=route)
