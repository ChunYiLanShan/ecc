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
from Queue import Queue, Empty
from multiprocessing.dummy import Pool
from threading import Thread

import oracle2mysql


class MySqlHistLoader(object):
    def __init__(self, mysql_adapter):
        self.mysql_adapter = mysql_adapter

    def get_hist_data(self, circuit_id):
        LATEST_COUNT = 7
        print "PID: %s, ID: %s\n" % (os.getpid(), circuit_id)
        return self.mysql_adapter.get_hist_electricity_circuit(
            self.mysql_adapter.new_connection(),
            circuit_id,
            LATEST_COUNT
        )


def do_work(task_queue):
    while True:
        try:
            task_data = task_queue.get(timeout=10)
            print "Get task:%s" % task_data[1]
            task_data[0](task_data[1])
            print "Done task"
            task_queue.task_done()
        except Empty:
            print "No new task. Just exit thread"
        return


def test_another_way():
    concurrency_factor = 10
    # prepare task queue
    task_queue = Queue(concurrency_factor)
    # prepare threads
    for i in range(concurrency_factor):
        t = Thread(target=do_work, args=(task_queue,))
        t.daemon = True
        t.start()

    mysql_adapter = oracle2mysql.MySqlAdatper()
    hist_data_loader = MySqlHistLoader(mysql_adapter)

    circuit_ids_temp = get_circuit_ids(mysql_adapter)
    try:
        for i in circuit_ids_temp:
            task_queue.put([hist_data_loader.get_hist_data, i])
        task_queue.join()
    except KeyboardInterrupt:
        sys.exit(1)



def get_circuit_ids(mysql_adapter):
    circuit_ids = mysql_adapter.get_circuit_ids()
    print "Got circuit_ids: %s" % circuit_ids
    return circuit_ids

def test_no_concurrency():
    mysql_adapter = oracle2mysql.MySqlAdatper()
    circuit_ids_in_mysql = get_circuit_ids(mysql_adapter)
    for i in circuit_ids_in_mysql:
        result = mysql_adapter.get_hist_electricity_circuit_no_concurrency(i, 7)
        print result

def test_basic_function():
    print os.getpid()
    mysql_adapter = oracle2mysql.MySqlAdatper()
    hist_data_loader = MySqlHistLoader(mysql_adapter)
    process_pool = Pool(20)
    circuit_ids_in_mysql = get_circuit_ids(mysql_adapter)
    hist_data_list = process_pool.map(hist_data_loader.get_hist_data, circuit_ids_in_mysql[:5])
    print hist_data_list

def test_get_hist():
    mysql_adapter = oracle2mysql.MySqlAdatper()
    print 'Start 17'
    result = mysql_adapter.get_hist_electricity_circuit(1,7)
    print result
    print 'End 17'

if __name__ == '__main__':
    test_no_concurrency()
