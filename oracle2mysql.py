# -*- coding: utf-8 -*-
import traceback
from datetime import datetime

import mysql.connector
import os
import time

import cx_Oracle
import sys

import datafit
from logutil import logger

DRY_RUN_MODE = False


def my_timer(func):
    def wrapper(*args, **kwargs):
        start = time.time()
        ret = func(*args, **kwargs)
        dur = time.time() - start
        logger.info("Duration of function %s: %s seconds" % (func.__name__, str(dur)))
        return ret

    return wrapper

def get_mysql_conn():
    host = os.environ['MYSQL_HOST']
    user = os.environ['MYSQL_USER']
    password = os.environ['MYSQL_PASSWORD']
    db_name = os.environ['MYSQL_DATABASE']

    logger.debug("connect to mysql database %s at host %s with user %s", db_name, host, user)
    return mysql.connector.connect(
        user=user,
        password=password,
        host=host,
        database=db_name
    )


class MySqlAdatper(object):

    def __init__(self, db_conn):
        self.db_conn = db_conn

    def get_all_equip_names(self):


        cursor = self.db_conn.cursor()

        query = ("SELECT id, name FROM energymanage_electricity_circuit ORDER BY id")


        cursor.execute(query)

        names = []
        for (id,name) in cursor:
            index_map = {'id':id, 'name':name}
            names.append(index_map)
        cursor.close()
        return names

    def get_all_water_equip_names(self):
        cursor = self.db_conn.cursor()

        query = ("SELECT id, branch_name FROM energymanage_water_circuit ORDER BY id")


        cursor.execute(query)

        names = []
        for (id, branch_name) in cursor:
            index_map = {'id':id, 'name':branch_name}
            names.append(index_map)
        cursor.close()
        return names

    def get_circuit_ids(self):
        cursor = self.db_conn.cursor()
        cursor.execute("select id from energymanage_electricity_circuit")
        try:
            return [circuit_id for (circuit_id,) in cursor]
        finally:
            cursor.close()

    def get_hist_electricity_circuit(self, circuit_id, latest_count):
        sql_query = '''SELECT id, circuit_id, time, voltage_A, voltage_B, voltage_C, current_A, current_B, current_C, power, quantity 
                        FROM energymanage_electricity_circuit_monitor_data 
                        WHERE circuit_id = %s ORDER BY time DESC LIMIT %s''' % (circuit_id, latest_count)
        logger.info("Query history data from MySQL for equip with circuit_id: %s, sql: %s." % (circuit_id, sql_query))
        cursor = self.db_conn.cursor()
        cursor.execute(sql_query)
        hist_energy_data_list = []
        try:
            row_dict_list = self._rows_to_dict_list(cursor)
            for row_dict in row_dict_list:
                hist_energy_data_list.append(
                    EquipEnergyData.build_from_dict(
                        row_dict['circuit_id'],
                        row_dict
                    )
                )

        finally:
            cursor.close()
        return hist_energy_data_list

    def insert_energy_point_data(self, equip_energy_data):
        add_energy = ("INSERT INTO energymanage_electricity_circuit_monitor_data"
                      "(voltage_A, voltage_B, voltage_C, current_A, current_B, current_C, quantity, power, time, circuit_id) "
                                    "VALUES (%(voltage_A)s, %(voltage_B)s, %(voltage_C)s, %(current_A)s, %(current_B)s, %(current_C)s, %(quantity)s, %(power)s, %(time)s,%(circuit_id)s)")
        sql = ''
        data_energy = {
                'voltage_A': equip_energy_data.voltage_A,
                'voltage_B': equip_energy_data.voltage_B,
                'voltage_C': equip_energy_data.voltage_C,
                'current_A': equip_energy_data.current_A,
                'current_B': equip_energy_data.current_B,
                'current_C': equip_energy_data.current_C,
                'quantity': equip_energy_data.quatity,
                'power': equip_energy_data.power,
                'time': datetime.now(),
                'circuit_id': equip_energy_data.mysql_equip_id
        }
        cursor = self.db_conn.cursor()

        cursor.execute(add_energy, data_energy)

        # Make sure data is committed to the database
        self.db_conn.commit()

        cursor.close()

    def insert_energy_point_data_in_batch(self, equip_energy_data_list):
        sql = """INSERT INTO energymanage_electricity_circuit_monitor_data 
                            (voltage_A, voltage_B, voltage_C, current_A, current_B, current_C, quantity, power, time, circuit_id) 
                            VALUES """

        values_list = []
        def to_sql_repr(value):
            if value is None:
                return 'NULL'
            else:
                return str(value)

        for equip_energy_data in equip_energy_data_list:
            new_row = "(" + to_sql_repr(equip_energy_data.voltage_A )
            new_row += "," + to_sql_repr(equip_energy_data.voltage_B)
            new_row += "," + to_sql_repr(equip_energy_data.voltage_C)
            new_row += "," + to_sql_repr(equip_energy_data.current_A)
            new_row += "," + to_sql_repr(equip_energy_data.current_B)
            new_row += "," + to_sql_repr(equip_energy_data.current_C)
            new_row += "," + to_sql_repr(equip_energy_data.quantity)
            new_row += "," + to_sql_repr(equip_energy_data.power)
            new_row += ",'" + to_sql_repr(datetime.now()) + "'"
            new_row += "," + to_sql_repr(equip_energy_data.mysql_equip_id)
            new_row += ")"
            values_list.append(new_row)
        values_sql = ",".join(values_list)
        sql += values_sql
        logger.info("SQL for new energy data: %s", sql)
        if DRY_RUN_MODE:
            return


        cursor = self.db_conn.cursor()

        cursor.execute(sql)

        # Make sure data is committed to the database
        self.db_conn.commit()

        cursor.close()

    def insert_water_energy_point_data_in_batch(self, eedl):
        sql = """INSERT INTO energymanage_water_circuit_monitor_data
                            (quantity, time, circuit_id) 
                            VALUES """

        values_list = []
        def to_sql_repr(value):
            if value is None:
                return 'NULL'
            else:
                return str(value)

        for equip_energy_data in eedl:
            new_row = "(" + to_sql_repr(equip_energy_data.quantity)
            new_row += ",'" + to_sql_repr(datetime.now()) + "'"
            new_row += "," + to_sql_repr(equip_energy_data.mysql_equip_id)
            new_row += ")"
            values_list.append(new_row)
        values_sql = ",".join(values_list)
        sql += values_sql
        logger.info("SQL for new energy data: %s", sql)


        cursor = self.db_conn.cursor()

        cursor.execute(sql)

        # Make sure data is committed to the database
        self.db_conn.commit()

        cursor.close()

    def clear(self):
        self.db_conn.close()

    # TODO: Dedup this method in OracleAdapter
    def _rows_to_dict_list(self, cursor):
        columns = [i[0] for i in cursor.description]
        return [dict(zip(columns, row)) for row in cursor]
        

class EquipEnergyData(object):
    FIELD_LIST = [
        'voltage_A',
        'voltage_B',
        'voltage_C',
        'current_A',
        'current_B',
        'current_C',
        'power',
        'quantity'
    ]

    @staticmethod
    def build_from_dict(mysql_equip_id, prop_to_value):
        energy_data = EquipEnergyData()
        energy_data.mysql_equip_id = mysql_equip_id
        for field in EquipEnergyData.FIELD_LIST:
            if field in prop_to_value.keys():
                setattr(energy_data, field, prop_to_value[field])
        return energy_data

    def __init__(self):
        self.mysql_equip_id = None
        self.oracle_equip_id = None
        self.name = None
        self.voltage_A = None
        self.voltage_B = None
        self.voltage_C = None
        self.current_A = None
        self.current_B = None
        self.current_C = None
        self.quantity = None
        self.power = None 

        #mapping data
        self.point_id_to_type = {} 

    def __str__(self):
        return str(self.__dict__)

class WaterEquipEnergyData(object):
    def __init__(self):
        self.mysql_equip_id = None
        self.oracle_equip_id = None
        self.name = None
        self.quantity = None
        self.point_id = None

    def __str__(self):
        return str(self.__dict__)


class OracleAdapter(object):
    ORACLE_SCHEMA = "xh"

    @staticmethod
    def get_oracle_conn():
        user = os.environ['ORACLE_USER']
        password = os.environ['ORACLE_PASSWORD']
        host = os.environ['ORACLE_HOST']
        port = os.environ['ORACLE_PORT']
        orcl_inst = os.environ['ORACLE_INSTANCE']

        # select chinese
        os.environ["NLS_LANG"] = "American_America.ZHS16GBK"
        # where chinese
        os.environ["NLS_LANG"] = "AMERICAN_AMERICA.UTF8"
        # Connect as user "hr" with password "welcome" to the "oraclepdb" service running on this computer.
        connection = cx_Oracle.connect(user, password, "%s:%s/%s" % (host, port, orcl_inst))
        return connection

    @staticmethod
    def is_oracle_available():
        #TODO
        is_oracle_ok = False
        conn = None
        try:
            conn = OracleAdapter.get_oracle_conn()
            logger.info('Oracle is available')
            is_oracle_ok = True
        except Exception:
            logger.error(traceback.format_exc())
            logger.error('Error. Oracle is not available.')
        finally:
            if conn is not None:
                conn.close()
        return is_oracle_ok

    def __init__(self, connection):
        self.connection = connection

    def _exe_sql(self, cursor, sql):
        cursor.execute(sql)
        for x in cursor:
            for y in x:
                if type(y) == type(u''):
                    sys.stdout.write(y.encode('utf-8'))
                else:
                    sys.stdout.write(str(y))
                sys.stdout.write('--')
            sys.stdout.write('\n')

    def get_data_for_equip(self, name, mysql_equip_id):
        '''
        EngeryPoint, {}, ...]
        '''
        equip_keys = self.get_equip_id_and_type(name)
        logger.info(equip_keys)
        result = self.get_rtm_point_ids(equip_keys['id'], equip_keys['type'])
        for point in result:
            # Get point current value
            point_value = self._get_point_val_from_rtm_controldata(point['point_id'])
            point['point_value'] = point_value
        equip_data = EquipEnergyData()
        equip_data.mysql_equip_id = mysql_equip_id
        equip_data.name = name
        equip_data.oracle_equip_id = '%s.%s' % (equip_keys['type'], equip_keys['id'])
        for point in result:
            setattr(equip_data, point['point_type'], point['point_value'])
        return equip_data


    def _rows_to_dict_list(self, cursor):
        columns = [i[0] for i in cursor.description]
        return [dict(zip(columns, row)) for row in cursor]

    def _get_point_val_from_rtm_controldata(self, point_id):
        sql = """SELECT * FROM hqliss1.RTM_CONTROLDATA WHERE POINT_ID = '%s'""" % point_id
        try:
            cursor = self.connection.cursor()
            i = 0
            cursor.execute(sql)
            rows_map = self._rows_to_dict_list(cursor)
            return rows_map[0]['RECORD']
        finally:
            cursor.close()

    def get_rtm_point_ids(self, equip_id, equip_type):
        '''
        有功功率
        有功功耗
        A相电压
        B相电压
        C相电压
        A相电流
        B相电流
        C相电流

        return:
            [ {'point_id':'', 'point_type':'xx'}, ... ]
            point_type: voltage_A,voltage_B, ...
            
        '''
        equip_no = '%s.%s' % (str(equip_type), str(equip_id))
        sql = """SELECT point_id, point_name, short_code, depict,equip_no FROM hqliss1.RTM_POINT WHERE equip_no = '%s'""" % equip_no
        voltage_a_str = u'A相电压'
        voltage_b_str = u'B相电压'
        voltage_c_str = u'C相电压'
        current_a_str = u'A相电流'
        current_b_str = u'B相电流'
        current_c_str = u'C相电流'
        power_str = u'有功功率'
        quatity_str = u'有功功耗'
        mysql_col_name_to_chinese_str = {
            'voltage_A':voltage_a_str,
            'voltage_B':voltage_b_str,
            'voltage_C':voltage_c_str,
            'current_A':current_a_str,
            'current_B':current_b_str,
            'current_C':current_c_str,
            'power':power_str,
            'quantity':quatity_str
        }
        def to_id_type_map(row_map):
            '''
            return {'point_id':'', 'point_type':''}
            '''

            point_name = row_map['POINT_NAME'] 
            depict = row_map['DEPICT']
            key_point_id = 'point_id'
            key_point_type = 'point_type'
            point_id_val = row_map['POINT_ID']

            for  k,v in mysql_col_name_to_chinese_str.items():
                if point_name == v or v in depict:
                    return {key_point_id:point_id_val, key_point_type:k}

            
        try:
            result = []
            cursor = self.connection.cursor()
            i = 0
            cursor.execute(sql)
            row_dict = self._rows_to_dict_list(cursor)
            for row_map in row_dict:
                id_type_map_result = to_id_type_map(row_map)
                if id_type_map_result is not None:
                    result.append(id_type_map_result)
            #validate result: make sure there are
            required_cols = mysql_col_name_to_chinese_str.keys()
            cols = []
            for x in result:
                cols.append(x['point_type'])
            for required_col in required_cols:
                if required_col not in cols:
                    logger.error('Not found required type: %s', required_col)
                    raise Exception('Not found required type: %s' % required_col)
                
            return result
        finally:
            cursor.close()

    def get_equip_id_and_type(self, name):
        sql = """SELECT EQUIP_ID, EQUIP_TYPE_ID FROM %s.EC_DEVICE_INFO WHERE equip_name = '%s'""" \
              % (OracleAdapter.ORACLE_SCHEMA, name)

        cursor = None
        try:
            cursor = self.connection.cursor()
            i = 0
            cursor.execute(sql)
            result = {}
            for x in cursor:
                i += 1
                result ={'id':x[0], 'type':x[1]}
            assert i == 1
            return result
        finally:
            if cursor is not None:
                cursor.close()

    def get_equip_name_to_ids(self, names):
        '''
        return {'name':'id', ...}
        '''
        names_sql = ','.join(map(lambda e : "'%s'" % e, names))
        sql = """SELECT id, name FROM %s.ec_device_info WHERE name IN (%s)""" \
              % (OracleAdapter.ORACLE_SCHEMA, names_sql)
        logger.info('SQL for get ec_device_info:%s', sql)
        try:
            cursor = self.connection.cursor()
            i = 0
            cursor.execute(sql)
            dict_list= self._rows_to_dict_list(cursor)
            result = {}
            for row_dict in dict_list:
                logger.info(row_dict)
                result[row_dict['NAME']] = row_dict['ID']
            if len(names) > len(result):
                logger.warn('Not found some corresponding equip based on name')
            logger.debug("equip_name_to_ids: %s" % str(result))
            return result
        finally:
            cursor.close()

    @staticmethod
    def check_point_type(row_dict):
        voltage_a_str = u'A相电压'
        voltage_b_str = u'B相电压'
        voltage_c_str = u'C相电压'
        current_a_str = u'A相电流'
        current_b_str = u'B相电流'
        current_c_str = u'C相电流'
        power_str = u'有功功率'
        quatity_str = u'有功功耗'
        mysql_col_name_to_chinese_str = {
            'voltage_A':voltage_a_str,
            'voltage_B':voltage_b_str,
            'voltage_C':voltage_c_str,
            'current_A':current_a_str,
            'current_B':current_b_str,
            'current_C':current_c_str,
            'power':power_str,
            'quantity':quatity_str
        }
        point_name = row_dict['NAME']
        point_depict = row_dict['POINTDESC']
        if point_name is not None :
            point_name = point_name.strip()
        if point_depict is not None:
            point_depict = point_depict.strip()

        logger.debug("point_name: %s, point_depict: %s." % (point_name, point_depict))
        for point_type_in_mysql, chinese_str in mysql_col_name_to_chinese_str.items():
            if point_name == chinese_str \
                    or point_depict.decode('utf-8').endswith(chinese_str):
                logger.debug("Got point type: %s" % point_type_in_mysql)
                return True, point_type_in_mysql
        logger.warn("Not get point type for point_depict: %s" % point_depict)
        return False, None

    def get_point_id_type(self, equip_id_list):
        '''
        input: ['type.id', 'type.id',...]
        return {'equip_id': {'point_id':'point_type', ... }, ... }
        '''

        equip_no_list = ','.join(map(lambda e : "'%s'" % e, equip_id_list))

        sql = """SELECT id, name, pointdesc, deviceinfo_id, projectpoint
                    FROM %s.ec_point_info WHERE deviceinfo_id IN (%s)""" \
              % (OracleAdapter.ORACLE_SCHEMA, equip_no_list)
        logger.info('SQL for get_point_id_type: %s', sql)

        try:
            cursor = self.connection.cursor()
            i = 0
            cursor.execute(sql)
            result = {}
            rows_list = self._rows_to_dict_list(cursor)
            for row_dict in rows_list:
                equip_id = row_dict['DEVICEINFO_ID']
                point_id = row_dict['PROJECTPOINT']
                is_need_type, point_type = OracleAdapter.check_point_type(row_dict)
                if is_need_type:
                    if equip_id in result:
                        point_id_to_type = result[equip_id]
                        point_id_to_type[point_id] = point_type
                    else:
                        result[equip_id] = { point_id: point_type }

            return result
        finally:
            cursor.close()

    def get_point_id_to_value(self, point_id_list):
        '''
        return {'point_id':'value', ... }
        '''
        point_ids_sql = ','.join(map(lambda e : "'%s'" % e, point_id_list))
        sql = """SELECT projectpoint, record FROM %s.ec_sdcd_data WHERE projectpoint IN (%s)""" \
              % (OracleAdapter.ORACLE_SCHEMA, point_ids_sql)
        logger.debug("get_point_id_to_value sql: %s" % sql)
        try:
            cursor = self.connection.cursor()
            cursor.execute(sql)
            result = {}
            rows_list = self._rows_to_dict_list(cursor)
            logger.debug("point_id_lsit %s" % point_id_list)
            logger.debug("rows_list: %s" % rows_list)
            for row_dict in rows_list:
                point_value = row_dict['RECORD']
                point_id = row_dict['PROJECTPOINT']
                result[point_id] = point_value
            for point_id_in_para in point_id_list:
                point_ids_got = result.keys()
                if point_id_in_para not in point_ids_got:
                    logger.warn("Not found point value for point id %s" % point_id_in_para)

            return result
        finally:
            cursor.close()


    # water 
    def get_equip_ids_from_water_equip_names(self, names):
        '''
        retur {'name':'equip_id'}
        '''
        names_sql = ','.join(map(lambda e : "'%s'" % e, names))
        sql = """SELECT EQUIP_ID, EQUIP_TYPE_ID, EQUIP_NAME FROM hqliss1.EQ_EQUIP WHERE equip_name IN (%s)""" % names_sql 
        logger.info('SQL:%s', sql)
        try:
            cursor = self.connection.cursor()
            i = 0
            cursor.execute(sql)
            dict_list= self._rows_to_dict_list(cursor)
            result = {}
            for row_dict in dict_list:
                result[row_dict['EQUIP_NAME']] = '%s.%s' % (row_dict['EQUIP_TYPE_ID'], row_dict['EQUIP_ID'])
            if len(names) > len(result):
                logger.warn('Not found some corresponding equip based on name')
            return result
        finally:
            cursor.close()

    def get_point_ids_from_water_equip_ids(self, equip_ids):
        '''
        return {'equip_no':'point_id',...}
        '''
        equip_no_list = ','.join(map(lambda e : "'%s'" % e, equip_ids))

        sql = """SELECT point_id, point_name, short_code, depict, equip_no FROM hqliss1.RTM_POINT WHERE equip_no IN (%s)""" % equip_no_list 
        try:
            cursor = self.connection.cursor()
            i = 0
            cursor.execute(sql)
            dict_list= self._rows_to_dict_list(cursor)
            result = {}
            for row_dict in dict_list:
                result[row_dict['EQUIP_NO']] = row_dict['POINT_ID']
            return result
        finally:
            cursor.close()


    def get_point_values_from_water_point_ids(self, point_ids):
        '''
        return {'point_id':'value',...}
        '''

        point_ids_sql = ','.join(map(lambda e : "'%s'" % e, point_ids))
        sql = """SELECT POINT_ID, RECORD FROM hqliss1.RTM_CONTROLDATA WHERE POINT_ID IN (%s)""" % point_ids_sql 
        try:
            cursor = self.connection.cursor()
            cursor.execute(sql)
            result = {}
            rows_list = self._rows_to_dict_list(cursor)
            for row_dict in rows_list:
                point_value = row_dict['RECORD']
                point_id = row_dict['POINT_ID']
                result[point_id] = point_value

            return result
        finally:
            cursor.close()
    def clear(self):
        self.connection.close()


def get_equip_engery_data_in_batch(oracle_adapter, equip_energy_data_list):
    '''
    indexes:
        [{'id':'', 'name':''}, ...]
    return:
        [EquipEnergyData, EquipEnergyData, ...]
    '''
    # Get oracle_equip_id and update equip_energy_data_list

    eedl = equip_energy_data_list
    name_to_ids = oracle_adapter.get_equip_name_to_ids([e.name for e in eedl])
    name_to_energy_data = dict(zip([e.name for e in eedl], eedl))
    for name in name_to_ids.keys():
        logger.debug(name)
        decode_name = name.decode('utf-8')
        logger.debug(decode_name)
        logger.debug('oralce: %s' % type(decode_name))
    decode_name_to_ids = {name.decode('utf-8'): id_v for name,id_v in name_to_ids.items()}

    for name, energy_data in name_to_energy_data.items():
        logger.debug('mysql: %s' % type(name))
        if name in decode_name_to_ids:
            energy_data.oracle_equip_id = decode_name_to_ids[name]
        else:
            # remove the entry whose name can't be found in oracle.
            pass
            logger.warn("Can't find equip name %s from oracle database.", name)
            eedl.remove(energy_data)

    logger.debug("Size of eedl matching names in mysql: %s" % len(eedl))
    # Get point_id -> point_type
    oracle_equip_id_to_point_id_type = oracle_adapter.get_point_id_type([e.oracle_equip_id for e in eedl])
    oracle_equip_id_to_energy_data = dict(zip([e.oracle_equip_id for e in eedl], eedl))
    for oracle_equip_id, energy_data in oracle_equip_id_to_energy_data.items():
        if oracle_equip_id in oracle_equip_id_to_point_id_type.keys():
            energy_data.point_id_to_type = oracle_equip_id_to_point_id_type[oracle_equip_id]
        else:
            logger.warn("Can't find equip number %s in rtm_point.", oracle_equip_id)
            eedl.remove(oracle_equip_id_to_energy_data[oracle_equip_id])

    # Get point value and update to energy data
    point_id_list = []
    for equip_id, point_id_to_point_type in oracle_equip_id_to_point_id_type.items():
        point_id_list += point_id_to_point_type.keys()

    point_id_to_value = oracle_adapter.get_point_id_to_value(point_id_list)
    point_id_to_energy_data = {}
    for e in eedl:
        point_id_to_type = e.point_id_to_type
        for point_id in point_id_to_type.keys():
            point_id_to_energy_data[point_id] = e

    for point_id, point_value in point_id_to_value.items():
        e = point_id_to_energy_data[point_id]
        setattr(e, e.point_id_to_type[point_id], point_value)


@my_timer
def collect_electricity():
    logger.debug('Start to collect electricity enegery data')
    mysqladapter = MySqlAdatper(get_mysql_conn())

    fit_tool = datafit.FittingTool(mysqladapter)
    if not OracleAdapter.is_oracle_available():
        logger.warn("Oracle is unavailable. Try to fit all.")
        fit_tool.fit_all()
        return

    oracle_adapter = OracleAdapter(OracleAdapter.get_oracle_conn())

    indexes = mysqladapter.get_all_equip_names()

    batch_size = 50
    import math
    step_cnt = int(math.ceil(len(indexes)/(batch_size*1.0)))
    for i in range(step_cnt):
        batch_indexes = indexes[i*batch_size: (i+1)*batch_size]

        logger.info("Round : %s, Electricity Equip count: %s", i, len(batch_indexes))
        id_names_str = "\n".join(['\t%s,%s' % (e['id'],e['name']) for e in batch_indexes])
        logger.info("Collect data for %s", id_names_str)
        start = time.time()

        #in batch style
        equip_energy_data_list = []
        for id_type_pair in batch_indexes:
            equip_energy_data = EquipEnergyData()
            equip_energy_data.mysql_equip_id = id_type_pair['id']
            equip_energy_data.name = id_type_pair['name']
            equip_energy_data_list.append(equip_energy_data)

        logger.info("Equipments count: %s", len(indexes))

        get_equip_engery_data_in_batch(oracle_adapter, equip_energy_data_list)
        fit_tool.fit_energy_data_when_no_update(equip_energy_data_list)
        mysqladapter.insert_energy_point_data_in_batch(equip_energy_data_list)
        logger.info("Round : %s complete in %s seconds", i, time.time() - start)

    oracle_adapter.clear()
    mysqladapter.clear()


def collect_water():
    mysql_adapter = MySqlAdatper(get_mysql_conn())
    oracle_adapter = OracleAdapter(OracleAdapter.get_oracle_conn())
    id_names = mysql_adapter.get_all_water_equip_names()

    names = []
    eedl = []
    for id_name_pair in id_names:
        equip_energy_data = WaterEquipEnergyData()
        equip_energy_data.mysql_equip_id = id_name_pair['id']
        equip_energy_data.name = id_name_pair['name']
        names.append(id_name_pair['name'])
        eedl.append(equip_energy_data)

    logger.info("Equipments count: %s", len(eedl))

    name_to_equip_id = oracle_adapter.get_equip_ids_from_water_equip_names(names)
    name_to_data = dict(zip([e.name for e in eedl], eedl))
    for k,v in name_to_equip_id.items():
        name_to_data[k].oracle_equip_id = v

    equip_id_to_data = dict(zip([e.oracle_equip_id for e in eedl], eedl))
    equip_id_to_point_id = oracle_adapter.get_point_ids_from_water_equip_ids(name_to_equip_id.values())
    for k, v in equip_id_to_point_id.items():
        equip_id_to_data[k].point_id = v

    point_id_to_value = oracle_adapter.get_point_values_from_water_point_ids(equip_id_to_point_id.values())
    point_id_to_data = dict(zip([e.point_id for e in eedl], eedl))
    for k, v in point_id_to_value.items():
        point_id_to_data[k].quantity = v
    
    mysql_adapter.insert_water_energy_point_data_in_batch(eedl)

    oracle_adapter.clear()
    mysql_adapter.clear()

def collect():
    collect_electricity()
    #Disable for upgrading to DB 2.5 only for electricity
    #collect_water()

def test_get_equip_name_to_ids():
    oracle_adapter = OracleAdapter(OracleAdapter.get_oracle_conn())
    names = [u'门诊楼B1层低配间A1L31柜螺杆机3号PE410R', u'科教楼B1层低配间行政楼空调PE410R']
    result = oracle_adapter.get_equip_name_to_ids(names)
    print '##'*50
    print result
    oracle_adapter.clear()

def test_get_point_id_type():
    oracle_adapter = OracleAdapter(OracleAdapter.get_oracle_conn())
    ids = ['8001.21719', '8001.21248']
    result = oracle_adapter.get_point_id_type(ids)
    print '##'*50
    print result
    for k,v in result.items():
        for y in v.keys():
            print y
    oracle_adapter.clear()

def test_get_point_id_to_value():
    oracle_adapter = OracleAdapter(OracleAdapter.get_oracle_conn())
    ids = ['310109.XH.8001.21719.21','310109.XH.8001.21719.12','310109.XH.8001.21719.9','310109.XH.8001.21719.8','310109.XH.8001.21719.16','310109.XH.8001.21719.3','310109.XH.8001.21719.2','310109.XH.8001.21719.1','310109.XH.8001.21719.10','310109.XH.8001.21248.8','310109.XH.8001.21248.9','310109.XH.8001.21248.1','310109.XH.8001.21248.21','310109.XH.8001.21248.3','310109.XH.8001.21248.16','310109.XH.8001.21248.12','310109.XH.8001.21248.2','310109.XH.8001.21248.10']
    result = oracle_adapter.get_point_id_to_value(ids)
    print '##'*50
    print result
    oracle_adapter.clear()

def test_get_equip_engery_data_in_batch():
    energy_data_list = []
    energy_data = EquipEnergyData()
    energy_data.name = u'门诊楼B1层低配间A1L31柜螺杆机3号PE410R'
    energy_data_list.append(energy_data)
    energy_data = EquipEnergyData()
    energy_data.name = u'科教楼B1层低配间行政楼空调PE410R'
    energy_data_list.append(energy_data)
    oracle_adapter = OracleAdapter(OracleAdapter.get_oracle_conn())
    get_equip_engery_data_in_batch(oracle_adapter, energy_data_list)
    for e in energy_data_list:
        print e
    oracle_adapter.clear()

def test_get_all_water_equip_names():
    mysqladapter = MySqlAdatper(get_mysql_conn())
    names = mysqladapter.get_all_water_equip_names()
    for x in names:
        for k,v in x.items():
            print k,v


def test_collect_electricity():
    import time
    time.sleep(10)
    collect_electricity()


def main():
    secs=5*60
    if 'ECC_DURATION' in os.environ:
        secs = int(os.environ['ECC_DURATION'])
    logger.info('Duration setting is %s', secs)
    
    while True:
        collect()
        logger.info('Sleep %s seconds for next run', secs)
        time.sleep(secs)


if __name__ == '__main__':
    try:
        main()
    except Exception as e:
        logger.exception(e)
        exit(1)
