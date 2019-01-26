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

import oracle2mysql


class MySqlHistLoader(object):
    def __init__(self, mysql_adapter, latest_count=7):
        self.mysql_adapter = mysql_adapter
        self.latest_count = latest_count

    def get_circuit_ids(self):
        circuit_ids = self.mysql_adapter.get_circuit_ids()
        print "Got circuit_ids: %s" % circuit_ids
        return circuit_ids

    def get_hist_data(self):
        circuit_ids_in_mysql = self.get_circuit_ids()
        hist_data = []
        for i in circuit_ids_in_mysql:
            result = self.mysql_adapter.get_hist_electricity_circuit(i, self.latest_count)
            hist_data.append(result)
        return hist_data


if __name__ == '__main__':
    sql_adapter = oracle2mysql.MySqlAdatper()
    hist_loader = MySqlHistLoader(sql_adapter)
    circuit_hist_data = hist_loader.get_hist_data()
    print circuit_hist_data
