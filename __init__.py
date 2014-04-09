"""algolib.lib.rabbit init"""
from fabric.context_managers import hide
from fabric.api import local
from fabric import api
import producer
import consumer
import json
import logging


ADMIN_USER = 'admin'
ADMIN_PASS = 'g0g0g0'
ADMIN_PORT = 55672


class Commands(object):
    """Rabbit MQ command templates"""
    BASE = './algolib/scripts/rabbitmqadmin --host=%s --port=%s --username=%s --password=%s'
    BIND_ROUTE = '%s -V %s declare binding source=%s destination_type=%s destination=%s routing_key=%s'
    DECLARE_EXCHANGE = '%s -V %s declare exchange name=%s type=%s'
    DECLARE_PERMISSIONS = '%s declare permission vhost=%s user=%s configure=.* write=.* read=.*'
    DECLARE_QUEUE = '%s -V %s declare queue name=%s'
    DECLARE_USER = '%s declare user name=%s password=%s tags='
    DECLARE_VHOST = '%s declare vhost name=%s'


def setup(config):
    """Creates Rabbit exchanges queues etc"""
    for section in [s for s in config.parser.sections() if 'rabbit_' in s]:
        rabbit = config.get_section(section)
        base_cmd = Commands.BASE % (rabbit['host'], ADMIN_PORT, ADMIN_USER, ADMIN_PASS)
        with hide('running', 'stdout'):
            logging.info('initializing rabbit\t%s', section)
            api.local(Commands.DECLARE_VHOST % (base_cmd, rabbit['vhost']), True)
            api.local(Commands.DECLARE_USER % (base_cmd, rabbit['username'], rabbit['password']), True)
            api.local(Commands.DECLARE_PERMISSIONS % (base_cmd, rabbit['vhost'], rabbit['username']), True)
            api.local(Commands.DECLARE_PERMISSIONS % (base_cmd, rabbit['vhost'], ADMIN_USER), True)
            api.local(Commands.DECLARE_EXCHANGE % (base_cmd, rabbit['vhost'], rabbit['exchange'], 'direct'), True)
            api.local(Commands.DECLARE_QUEUE % (base_cmd, rabbit['vhost'], rabbit['queue_name']), True)
            if rabbit.get('route'):
                api.local(Commands.BIND_ROUTE % (base_cmd, rabbit['vhost'], rabbit['exchange'], 'queue', rabbit['queue_name'], rabbit['route']), True)
            if "additional_routes" in rabbit:
                for route in json.loads(rabbit["additional_routes"]):
                    api.local(Commands.BIND_ROUTE % (base_cmd, rabbit["vhost"], rabbit["exchange"], "queue", rabbit["queue_name"], route), True)
