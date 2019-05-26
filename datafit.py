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
from decimal import Decimal, ROUND_DOWN
from logutil import logger


class MySqlHistLoader(object):
    def __init__(self, mysql_adapter, latest_count=7):
        self.mysql_adapter = mysql_adapter
        self.latest_count = latest_count

    def get_circuit_ids(self):
        circuit_ids = self.mysql_adapter.get_circuit_ids()
        logger.info("Got circuit_ids: %s" % circuit_ids)
        return circuit_ids

    def get_hist_data(self):
        """
        Get history energy data for all electronic equipments.
        If there is no history data, the hist data will be a empty list.
        The hist data for given equipment is in time's DESC order. Its hist values should be also in desc
        order. Suppose all related data won't decrease.
        :return: A tuple with below info:
        (
        [
            [equip1_data1, equip1_data2],
            [equip2_data1, equip2_data2],
            []
            ...
        ]
        ,
        {
            'equip1':[equip1_data1, equip1_data2],
            'equip2':[equip2_data1, equip2_data2],
            'equip3': []
            ...
        }
        )
        """
        logger.info("get_hist_data started.")
        circuit_ids_in_mysql = self.get_circuit_ids()
        hist_data = []
        hist_data_dict = {}
        for circuit_id in circuit_ids_in_mysql:
            hist_energy_data = self.mysql_adapter.get_hist_electricity_circuit(
                circuit_id,
                self.latest_count
            )

            hist_data_dict[circuit_id] = hist_energy_data
            hist_data.append(hist_energy_data)
        logger.info("get_hist_data completed.")
        return hist_data, hist_data_dict


class FittingTool(object):
    def __init__(self, mysql_adapter):
        logger.info("FittingTool.__init__ started.")
        self.mysql_adapter = mysql_adapter
        self.hist_loader = MySqlHistLoader(self.mysql_adapter)
        self.hist_energy_data, self.hist_energy_data_dict = self.hist_loader.get_hist_data()
        self.fitted_energy_data_dict = self.fit_hist_energy_data(self.hist_energy_data_dict)
        logger.info("FittingTool.__init__ completed.")

    def fit_all(self):
        # indexes = self.mysql_adapter.get_all_equip_names()
        # equip_energy_data_list = []
        # for id_type_pair in indexes:
        #     equip_energy_data = oracle2mysql.EquipEnergyData()
        #     equip_energy_data.mysql_equip_id = id_type_pair['id']
        #     equip_energy_data.name = id_type_pair['name']
        #     equip_energy_data_list.append(equip_energy_data)

        # oracle2mysql.logger.info("Equipments count: %s", len(indexes))
        # Remove None
        logger.info("fit_all started.")
        self.mysql_adapter.insert_energy_point_data_in_batch(self.fitted_energy_data_dict.values())
        logger.info("fit_all completed.")

    def fit_energy_data_when_no_update(self, equip_energy_data_list):
        """
        For given list of EquipEnergyData, patch it with fitted data if needed.
        :param equip_energy_data_list:
        :return:
        """
        logger.info("fit_energy_data_when_no_update started.")
        for equip_energy_data in equip_energy_data_list:
            for field in oracle2mysql.EquipEnergyData.FIELD_LIST:
                if FittingTool.need_fit(equip_energy_data, self.hist_energy_data_dict, field):
                    logger.info("Try to fit field %s data for circuit id %s"
                                % (field, equip_energy_data.mysql_equip_id))
                    fitted_field_val = getattr(
                        self.fitted_energy_data_dict[equip_energy_data.mysql_equip_id],
                        field
                    )
                    setattr(
                        equip_energy_data,
                        field,
                        fitted_field_val
                    )
                    equip_energy_data.istrue = 0
        logger.info("fit_energy_data_when_no_update completed.")

    @staticmethod
    def need_fit(equip_energy_data, hist_energy_data_dict, field_name):
        """
        Check if this property of EquipEnergyData needs fitting.
        :param equip_energy_data:
        :param hist_energy_data_dict:
        :param field_name:
        :return:
        """
        circuit_id = equip_energy_data.mysql_equip_id
        if len(hist_energy_data_dict[circuit_id]) == 0:
            logger.warn("There is no history data for %s. Can't do data fit." % circuit_id)
            return False

        field_collected_val = getattr(equip_energy_data, field_name)
        none_collected = field_collected_val is None
        if none_collected:
            logger.warn("Circuit id: %s, field: %s, its value is None, which meaning can't collect from oracle."
                        % (circuit_id, field_name))
            return True

        field_latest_imported_val = getattr(
            hist_energy_data_dict[circuit_id][0],
            field_name
        )
        
        data_changed = (field_collected_val != field_latest_imported_val)
        if not data_changed:
            logger.info(
                "Need fit data for field %s, field_collected_val = field_latest_imported_val = %s" %
                (field_name, field_collected_val)
            )
        return not data_changed

    def fit_hist_energy_data(self, hist_energy_data_dict):
        """
        For all history energy data of equipments, fit the data respectively.
        :param hist_energy_data: List of List [ [equip_1_hist_1, equip_1_hist_2], [equip_2_hist_1, equip_2_hist_2],... ]
        :return: key is equip id, value is fitted EquipEnergyData instance.
        """
        logger.info("fit_hist_energy_data started.")
        fitted_enery_data_dict = {
            equip_id: self.fit_energy_data(hist_data)
            for equip_id, hist_data in hist_energy_data_dict.items()
        }

        # Remove items whose fitted data is none.
        fitted_enery_data_dict_without_none = {}
        for equip_id, fitted_data in fitted_enery_data_dict.items():
            if fitted_data is None:
                logger.info("No history data for circuit id: %s" % equip_id)
            else:
                fitted_enery_data_dict_without_none[equip_id] = fitted_data

        logger.info("fit_hist_energy_data completed.")
        return fitted_enery_data_dict_without_none

    @staticmethod
    def fit_energy_data(energy_data_hist_for_single_equip):
        """
        Based on given history energy data for a specific equipment,
        fitting all related properties of EquipEnergyData from the history data.
        :param energy_data_hist_for_single_equip:
        :return: a fitted EquipEnergyData object,
                or None when input is a empty list`
        """
        if len(energy_data_hist_for_single_equip) == 0:
            return None

        fitted_energy_data = oracle2mysql.EquipEnergyData()
        ids = [energy_data.mysql_equip_id for energy_data in energy_data_hist_for_single_equip]
        assert len(set(ids)) == 1

        fields = oracle2mysql.EquipEnergyData.FIELD_LIST
        fitted_energy_data.mysql_equip_id = ids[0]
        for field in fields:
            field_vals = map(
                lambda obj: getattr(obj, field),
                energy_data_hist_for_single_equip
            )

            field_vals_not_none = [e for e in field_vals if e is not None]

            if len(field_vals_not_none) == 0:
                err_msg = "ERROR: circuit id %s, its field %s history data is None. " \
                      "Data is %s" % (fitted_energy_data.mysql_equip_id, field, field_vals)
                logger.warn(err_msg)
                fitted_val = None
            else:
                desc_order = all(
                    [
                        field_vals_not_none[i] >= field_vals_not_none[i + 1]
                        for i in xrange(len(field_vals_not_none) - 1)
                    ]
                )
                # Not required.
                # if desc_order is not True:
                #     err_msg = "ERROR: circuit id %s, its field %s history data is not in desc order. " \
                #               "Data is %s" % (fitted_energy_data.mysql_equip_id, field, field_vals_not_none)
                #     logger.error(err_msg)
                #     raise Exception(err_msg)
                try:
                    is_descending = FittingTool.is_descending(field_vals_not_none)
                    fitted_val = None
                    if is_descending:
                        fitted_val = FittingTool.fit_data(field_vals_not_none)
                    else:
                        fitted_val = FittingTool.fit_data_v2(field_vals_not_none)
                    fit_msg = "Circuit id %s,  field: %s, fitted data: %s, is descending: %s, raw data is %s. "\
                              % (fitted_energy_data.mysql_equip_id, field, str(fitted_val), is_descending,
                                 field_vals_not_none)
                    logger.info(fit_msg)
                except Exception as e:
                    err_msg = "Circuit id %s, error when fit its field %s history data . " \
                              "Data is %s. " \
                              "error is: %s" % (fitted_energy_data.mysql_equip_id, field, field_vals_not_none, e.message)
                    logger.error(err_msg)
                    fitted_val = field_vals_not_none[0]

            setattr(fitted_energy_data, field, fitted_val)

        return fitted_energy_data

    @staticmethod
    def fit_data_v2(data_list):
        data_list_without_none = [d for d in data_list if d is not None]
        mean_val = sum(data_list_without_none)/len(data_list_without_none)
        return Decimal(mean_val).quantize(Decimal('.01'), rounding=ROUND_DOWN)

    @staticmethod
    def is_descending(data_list):
        if len(data_list) == 0 or len(data_list) == 1:
            return True
        for i in range(0, len(data_list)-1):
            if data_list[i] < data_list[i+1]:
                return False
        return True

    @staticmethod
    def fit_data(data_list):
        """
        The data list may not be in desc order, since maybe the fit data is bigger than the next real data.
        Just to check if the mean of delta is not negative.
        The first element is the latest data.
        Fit the data using a simple method. Refer to return.
        :param data_list:
        :return:  = data_list[0] + mean(delta(data_list))
        """
        data_list_without_none = [d for d in data_list if d is not None]
        assert len(data_list_without_none) > 0
        if len(data_list_without_none) == 1:
            return data_list_without_none[0]
        # Compute the delta of adjacent elements, e.g.  [1, 3, 9, 10] => [(3-1), (9-3), (10-9)] => [2, 6, 1]
        lst = data_list_without_none # use a short name: lst
        delta = [
            lst[i]-lst[i+1]
            for i in range(0, len(lst) - 1)
        ]

        mean_delta = sum(delta)/len(delta)
        if mean_delta < 0:
            err_msg = 'mean of delta is negative. Data values:%s ' % lst
            logger.error(err_msg)
            raise Exception(err_msg)
        fitted_data = data_list[0] + mean_delta
        return Decimal(fitted_data).quantize(Decimal('.01'), rounding=ROUND_DOWN)


if __name__ == '__main__':
    sql_adapter = oracle2mysql.MySqlAdatper(oracle2mysql.get_mysql_conn())
    hist_loader = MySqlHistLoader(sql_adapter)
    circuit_hist_data = hist_loader.get_hist_data()
    print circuit_hist_data
