# -*- coding: utf-8 -*-

'''
Load history data from MySQL.
What I want to do is
SELECT
   GROUP_CONCAT(energy.voltage_A ORDER BY time DESC SEPARATOR ',' LIMIT 7) as latest_7_voltage_A,
   ...
FROM energymanage_electricity_circuit_monitor_data energy
GROUP BY circuit_id;

However, MySQL doesn't support LIMIT in aggregation function GROUP_CONCAT. So I have to use below algorithm
as workaround:

hist_data = []
for circuit_id_val in ('SELECT id IN energymanage_electricity_circuit'):
    energy_data = `SELECT * FROM energymanage_electricity_circuit
                    WHERE circuit_id = $circuit_id_val
                    ORDER BY time DESC LIMIT 7;`
    hist_data.append(energy_data)
return hist_data


The complexity of such workaround is O(n) where n is the count of energymanage_electricity_circuit.
For fast query speed, we need the help from concurrency.
'''
import os
import sys
import time
from Queue import Queue
from multiprocessing.dummy import Pool
from threading import Thread

import oracle2mysql


class MySqlHistLoader(object):
    pass


def do_work():
    pass


def concurrent_test(req):
    concurrency_factor = 10
    # prepare task queue
    task_queue = Queue(concurrency_factor)
    # prepare threads
    for i in range(concurrency_factor):
        t = Thread(target=do_work, args=(task_queue,))
        t.daemon = True
        t.start()
    try:
        for i in range(1000):
            task_queue.put([req])
            task_queue.join()
    except KeyboardInterrupt:
        sys.exit(1)


def get_hist_data(circuit_id):
    print "PID: %s, ID: %s\n" % (os.getpid(), circuit_id)
    time.sleep(1)
    return 1


def get_circuit_ids(mysql_adapter):
    circuit_ids = range(1,417)
    circuit_ids = mysql_adapter.get_circuit_ids()
    print "Got circuit_ids: %s" % circuit_ids
    return circuit_ids


if __name__ == '__main__':
    print os.getpid()
    mysql_adapter = oracle2mysql.MySqlAdatper()
    process_pool = Pool(20)
    hist_data_list = process_pool.map(get_hist_data, get_circuit_ids(mysql_adapter))
    print hist_data_list
