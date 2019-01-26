# -*- coding: utf-8 -*-

"""
There are two jobs for data fitting:
    1. Load history data for data fitting;
    2. Data fitting.

1. Load history data from MySQL.
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

2. data fitting
"""

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
        hist_data_dict = {}
        for circuit_id in circuit_ids_in_mysql:
            energy_data = self.to_energy_data(
                self.mysql_adapter.get_hist_electricity_circuit(circuit_id, self.latest_count)
            )
            hist_data_dict[circuit_id] = energy_data
            hist_data.append(energy_data)
        return hist_data, hist_data_dict


class FittingTool(object):
    def __init__(self, mysql_adapter):
        hist_loader = MySqlHistLoader(mysql_adapter)
        self.hist_energy_data, self.hist_energy_data_dict = hist_loader.get_hist_data()
        self.fitted_energy_data_dict = self.fit_hist_energy_data(self.hist_energy_data)

    def fit_hist_energy_data(self, hist_energy_data):
        fitted_energy_data = map(
            self.fit_energy_data,
            hist_energy_data
        )
        return dict(
            zip(
                [e.mysql_equip_id for e in fitted_energy_data],
                fitted_energy_data
            )
        )

    def fit_all(self):
        return map(self.fit_energy_data, self.hist_energy_data)

    @staticmethod
    def need_fit(equip_energy_data, hist_energy_data_dict, field_name):
        circuit_id = equip_energy_data.mysql_equip_id
        field_collected_val = getattr(equip_energy_data, field_name)
        field_latest_imported_val = getattr(
            hist_energy_data_dict[circuit_id][0],
            field_name
        )
        return field_collected_val == field_latest_imported_val

    def fit_energy_data_when_no_update(self, equip_energy_data_list):
        for equip_energy_data in equip_energy_data_list:
            for field in oracle2mysql.EquipEnergyData.FIELD_LIST:
                if FittingTool.need_fit(equip_energy_data, self.hist_energy_data_dict, field):
                    setattr(equip_energy_data,
                            field,
                            self.fitted_energy_data_dict[equip_energy_data.mysql_equip_id]
                            )

    def fitting_all_circuits_to_dict(self, hist_data_list):
        fitted_data = self.fitting_all_circuits(hist_data_list)
        mysql_equip_ids = map(lambda energy_data: energy_data.mysql_equip_id, fitted_data)
        return dict(
            zip(
                mysql_equip_ids,
                fitted_data
            )
        )

    @staticmethod
    def fit_energy_data(energy_data_hist_for_single_equip):
        fitted_energy_data = oracle2mysql.EquipEnergyData()
        fields = oracle2mysql.EquipEnergyData.FIELD_LIST

        for field in fields:
            field_vals = map(
                lambda obj: getattr(obj, field),
                energy_data_hist_for_single_equip
            )
            fitted_val = FittingTool.fit_data(field_vals)
            setattr(fitted_energy_data, field, fitted_val)

        return fitted_energy_data

    @staticmethod
    def fit_data(imported_data_list):
        assert len(imported_data_list) > 0
        if len(imported_data_list) == 1:
            return imported_data_list[0]
        # Compute the delta of adjacent elements, e.g.  [1, 3, 9, 10] => [(3-1), (9-3), (10-9)] => [2, 6, 1]
        lst = imported_data_list # use a short name: lst
        delta = [
            lst[i]-lst[i-1]
            for i in range(1, len(lst))
        ]

        mean_delta = sum(delta)/len(delta)
        fitted_data = imported_data_list[-1] + mean_delta
        return fitted_data




if __name__ == '__main__':
    sql_adapter = oracle2mysql.MySqlAdatper()
    hist_loader = MySqlHistLoader(sql_adapter)
    circuit_hist_data = hist_loader.get_hist_data()
    print circuit_hist_data
