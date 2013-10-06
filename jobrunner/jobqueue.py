# vim: tabstop=4 shiftwidth=4 softtabstop=4

# Copyright 2013 Cloudbase Solutions Srl
# All Rights Reserved.
#
#    Licensed under the Apache License, Version 2.0 (the "License"); you may
#    not use this file except in compliance with the License. You may obtain
#    a copy of the License at
#
#         http://www.apache.org/licenses/LICENSE-2.0
#
#    Unless required by applicable law or agreed to in writing, software
#    distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
#    WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
#    License for the specific language governing permissions and limitations
#    under the License.

import sys
import amqpclient
import time

from oslo.config import cfg
from jobrunner.openstack.common import log as logging

opts = [
    cfg.StrOpt(
        'queue',
        default='jobqueue',
        help='RabbitMQ host'),

]

opts_wrk = [
    cfg.StrOpt(
        'queue',
        default='jobqueue',
        help='RabbitMQ host'),

]

CONF = cfg.CONF
CONF.register_opts(opts, 'jobqueue')
CONF.register_opts(opts, 'jobworker')


LOG = logging.getLogger(__name__)


def broker_opts():
    return {
        "host": CONF.rabbitMQ.host,
        "user": CONF.rabbitMQ.user,
        "passwd": CONF.rabbitMQ.passwd,
        "vhost": CONF.rabbitMQ.vhost,
        "retry": CONF.rabbitMQ.retry,
    }


def callback(msg):
    OPTS = broker_opts()
    pr = amqpclient.Producer(CONF.rabbitMQ.exchange, **OPTS)
    pr.publish(msg.body, CONF.jobworker.queue)
    pr.close()
    return True


def main():
    CONF(sys.argv[1:])
    logging.setup('jobqueue')
    OPTS = broker_opts()
    while True:
        try:
            c = amqpclient.Consume(
                CONF.rabbitMQ.exchange,
                CONF.jobqueue.queue,
                **OPTS)
            c.consume(callback)
        except Exception as err:
            LOG.error(err)
            time.sleep(3)


if __name__ == "__main__":
    main()
