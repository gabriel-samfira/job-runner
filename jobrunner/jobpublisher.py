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

import flask
import json
import sys
import uuid
import amqpclient

from oslo.config import cfg
from jobrunner.openstack.common import log as logging

opts = [
    cfg.StrOpt(
        'auth_key',
        default='',
        help='Key used to authenticate client requests'),
    cfg.IntOpt(
        'http_port',
        default=4000,
        help='Http listen port'),
    cfg.StrOpt(
        'http_host',
        default='127.0.0.1',
        help='Http listen host'),
]

opts_queue = [
    cfg.StrOpt(
        'queue',
        default='jobqueue',
        help='RabbitMQ host'),

]

CONF = cfg.CONF
CONF.register_opts(opts, 'jobpublisher')
CONF.register_opts(opts_queue, 'jobqueue')

LOG = logging.getLogger(__name__)
app = flask.Flask(__name__)


def broker_opts():
    return {
        "host": CONF.rabbitMQ.host,
        "user": CONF.rabbitMQ.user,
        "passwd": CONF.rabbitMQ.passwd,
        "vhost": CONF.rabbitMQ.vhost,
        "retry": CONF.rabbitMQ.retry,
    }


def enqueue_job(data):
    OPTS = broker_opts()
    pr = amqpclient.Producer(CONF.rabbitMQ.exchange, **OPTS)
    pr.publish(json.dumps(data), CONF.jobqueue.queue)
    pr.close()
    return True


@app.route('/jobs/new', methods=['POST'])
def new_job():
    request_data = flask.request.json

    auth_key = request_data['auth_key']
    if auth_key != CONF.jobpublisher.auth_key:
        raise Exception('The provided auth_key is not valid')

    job_id = str(uuid.uuid4())

    # Copy relevant dict content
    data = {}
    data['job_name'] = request_data.get('job_name', 'default')
    data['job_args'] = request_data.get('job_args', [])
    data['return_url'] = request_data.get('return_url', None)
    data['results_email'] = request_data.get('results_email', None)
    data['job_id'] = job_id

    enqueue_job(data)
    return json.dumps({'job_id': job_id})


def main():
    CONF(sys.argv[1:])
    logging.setup('jobpublisher')

    if not CONF.jobpublisher.auth_key:
        raise Exception("auth_key not set")

    app.run(host=CONF.jobpublisher.http_host,
            port=CONF.jobpublisher.http_port,
            debug=False)

if __name__ == '__main__':
    main()
