import amqp
import json
import datetime
import socket

from amqp.exceptions import *
from oslo.config import cfg

import logging
log = logging.getLogger(__name__)

_HOSTNAME_ = socket.getfqdn()

opts = [
    cfg.StrOpt(
        'host',
        default='127.0.0.1',
        help='RabbitMQ host'),
    cfg.StrOpt(
        'user',
        default='',
        help='RabbitMQ user'),
    cfg.StrOpt(
        'passwd',
        default='',
        help='RabbitMQ passwd'),
    cfg.StrOpt(
        'vhost',
        default='',
        help='RabbitMQ vhost'),
    cfg.IntOpt(
        'retry',
        default=30,
        help='RabbitMQ connect retry interval'),
    cfg.StrOpt(
        'exchange',
        default='',
        help='RabbitMQ exchange'),
]

CONF = cfg.CONF
CONF.register_opts(opts, 'rabbitMQ')


class Connection(object):

    def __init__(self, host='127.0.0.1', user='guest',
                 passwd='guest', vhost='/', ssl=None, retry=30):
        self.host = host
        self.user = user
        self.passwd = passwd
        self.vhost = vhost
        self.ssl = ssl
        self.retry = retry
        self.is_connected = False
        self.stamp = datetime.datetime.utcnow().strftime('%s')
        self.connect()

    def connect(self):
        try:
            self.connection = amqp.Connection(
                host=self.host,
                userid=self.user,
                password=self.passwd,
                virtual_host=self.vhost,
                ssl=self.ssl,
                insist=False
            )
            self.is_connected = True
        except Exception as err:
            log.exception(err)
            self.is_connected = False
        return self

    def is_alive(self):
        if self.is_connected:
            try:
                ch = self.connection.channel()
                ch.close()
            except:
                self.is_connected = False
                return False
            return True
        self.is_connected = False
        return False

    def recreate(self):
        if self.is_alive():
            return self

        while 1:
            if self.is_connected is False:
                self.connect()
                if self.is_connected is True:
                    self.channel = self.connection.channel()
                    break
                time.sleep(int(self.retry))
            else:
                break
        return self


class BaseQueue(Connection):

    def declare_exchange(self, durable=True, auto_delete=False):
        self.channel.exchange_declare(
            exchange=self.exchange_name,
            type='direct',
            durable=durable,
            auto_delete=auto_delete)
        return True

    def declare_queue(self, queue_name, routing_key,
                      durable=True, exclusive=False, auto_delete=False):

        self.queue_name = queue_name
        self.routing_key = routing_key
        self.channel.queue_declare(
            queue=self.queue_name,
            durable=durable,
            exclusive=exclusive,
            auto_delete=auto_delete)
        self.channel.queue_bind(
            queue=self.queue_name,
            exchange=self.exchange_name,
            routing_key=self.routing_key)


class Consume(BaseQueue):

    def __init__(self, exchange_name, myQueue, *args, **kw):
        self.Q = myQueue
        self.exchange_name = exchange_name
        super(Consume, self).__init__(*args, **kw)
        self.ext_callback = None
        if self.is_alive() is False:
            if self.recreate() is False:
                raise IOError('Failed to connect to RabbitMQ server')
        self.channel = self.connection.channel()
        #declare exchange
        self.declare_exchange()
        self.declare_queue(self.Q, self.Q)

    def wrapper_callback(self, msg):
        try:
            self.ext_callback(msg)
            self.channel.basic_ack(msg.delivery_tag)
        except Exception as err:
            log.exception(err)

    def consume(self, callback):
        self.ext_callback = callback
        self.process_waiting_msgs(callback)
        # start consuming
        self.channel.basic_consume(
            queue=self.Q,
            callback=self.wrapper_callback,
            consumer_tag=_HOSTNAME_)
        while 1:
            try:
                self.channel.wait()
            except Exception as err:
                log.exception(err)
                break
        return False

    def process_waiting_msgs(self, callback):
        if self.is_alive() is False:
            if self.recreate() is False:
                raise IOError('Failed to connect to RabbitMQ server')
        while 1:
            reply_sent = False
            msg = self.channel.basic_get(self.Q)
            if msg is None:
                break
            try:
                callback(msg)
                self.channel.basic_ack(msg.delivery_tag)
            except Exception as err:
                log.exception(err)
        return True

    def cancel_consume(self):
        self.channel.basic_cancel(_HOSTNAME_)
        return True

    def close(self):
        self.channel.close()
        self.connection.close()


class Producer(BaseQueue):

    def __init__(self, exchange_name, myQueue=None, *args, **kw):
        self.exchange_name = exchange_name
        self.Q = myQueue
        super(Producer, self).__init__(*args, **kw)
        #create channel
        if self.is_alive() is False:
            if self.recreate() is False:
                raise IOError('Failed to connect to RabbitMQ server')
        self.channel = self.connection.channel()
        #declare exchange
        self.declare_exchange()

    def check_message(self, msg):
        try:
            m = json.loads(msg)
        except Exception as err:
            log.exception(err)
            raise ValueError
        return m

    def generate_headers(self):
        hostname = _HOSTNAME_
        tstamp = int(datetime.datetime.utcnow().strftime('%s'))
        return {'hostname': hostname, 'time': tstamp}

    def publish(self, message, routing_key,
                message_id=None, extra_headers=None):
        #check message
        m = self.check_message(message)
        #convert dict to json
        tmp = json.dumps(m, indent=2)
        #declare remote queue
        self.declare_queue(routing_key, routing_key)
        #declare return queue. This queue will be used to receive responses
        if self.Q is not None:
            self.declare_queue(self.Q, self.Q)

        #create amqp message
        headers = self.generate_headers()
        if extra_headers is not None:
            if type(extra_headers) is dict:
                headers.update(extra_headers)

        msg = amqp.Message(tmp, application_headers=headers)
        msg.properties["content_type"] = "application/json"
        msg.properties["delivery_mode"] = 2
        if message_id:
            msg.properties['message_id'] = message_id

        if self.Q is not None:
            #add reply_to field
            msg.properties['reply_to'] = self.Q

        self.channel.basic_publish(
            exchange=self.exchange_name,
            routing_key=routing_key,
            msg=msg)
        return True

    def close(self):
        self.channel.close()
        self.connection.close()
