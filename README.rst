Cloudbase Job-Runner
====================

Lightweight application created for running jobs on demand with a RESTful API interface and a scalable backend. Job queues are based on Zeromq.

Setup on RHEL 6.4 or CentOS 6.4
-------------------------------

Dependencies::

 wget http://dl.fedoraproject.org/pub/epel/6/x86_64/epel-release-6-8.noarch.rpm
 wget http://rpms.famillecollet.com/enterprise/remi-release-6.rpm
 rpm -Uvh remi-release-6*.rpm epel-release-6*.rpm

 yum --enablerepo=epel-testing install python-amqp
 yum install -y python-flask python-netaddr python-six python-iso8601 python-eventlet rabbitmq-server
 yum install -y python-d2to1

 chkconfig --add rabbitmq-server
 chkconfig --level 2345 rabbitmq-server on
 /etc/init.d/rabbitmq-server start

Setup::

 python setup.py install

 cp scripts/* /etc/init.d/
 useradd -M jobrunner
 mkdir /var/log/jobrunner
 chown jobrunner.jobrunner /var/log/jobrunner/ 
 mkdir /var/run/jobrunner
 chown jobrunner.jobrunner /var/run/jobrunner/
 mkdir /etc/jobrunner
 cp conf/jobrunner.conf.sample /etc/jobrunner/jobrunner.conf

Start the services based on your configuration. For example:

Frontend / web node::

 service cloudbase-job-queue start
 service cloudbase-job-publisher start


Backend worker node::

 service cloudbase-job-worker start


On RHEL, Fedora, CentOS, SL, you can set the service to start automatically::

 chkconfig cloudbase-job-queue on
 chkconfig cloudbase-job-publisher on
 chkconfig cloudbase-job-worker on

Note: the jobs will run with the "jobrunner" account, verify permissions accordingly.

