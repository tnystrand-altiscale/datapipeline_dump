# Needed: export PYTHONPATH='/opt/hive-2.1.1/lib/py/'
# Don't try to manually install the libs.... a .h file is missing for sasl build
# Tried to find this file online and install...just ran down a rabbit hole of errors
# Test edit
import sys, os
print os.getenv('PYTHONPATH')

from thrift import Thrift
from thrift.transport import TSocket
from thrift.transport import TTransport
from thrift.protocol import TBinaryProtocol

from hive_service import ThriftHive
from hive_service.ttypes import HiveServerException

import pdb

try:
    transport = TSocket.TSocket(r'desktop2-dogfood.service.altiscale.com', 28150)
    transport = TTransport.TBufferedTransport(transport)
    protocol = TBinaryProtocol.TBinaryProtocol(transport)

    client = ThriftHive.Client(protocol)
    transport.open()


    print client.hi()

    print 'loads of things'
    client.getClusterStatus()

    print 'Showing databases'
    #pdb.set_trace()
    client.execute("show databases")
    print client.fetchAll()
    print 'Selecting from table'
    print 'Selecting from table'
    print 'Selecting from table'
    print 'Selecting from table'
    print 'Selecting from table'
    print 'Selecting from table'
    client.execute("select count(*) from cluster_metrics_prod_2.burst_time_series_patchjoin")
    while (1):
        print 'looping'
        row = client.fetchOne()
        if (row == None):
          break
        print row
    client.execute("show databases")
    print client.fetchAll()

    transport.close()

except Thrift.TException, tx:
    print '%s' % (tx.message)
