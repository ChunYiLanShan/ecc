# -*- coding: utf-8 -*-

import datetime
import mysql.connector
import logging
import os
import sys

#oracle
import cx_Oracle

logger = logging.getLogger()
handler = logging.StreamHandler()
formatter = logging.Formatter(
        '%(asctime)s %(name)-12s %(levelname)-8s %(message)s')
handler.setFormatter(formatter)
logger.addHandler(handler)
logger.setLevel(logging.DEBUG)


class MySqlAdatper(object):


    def __init__(self, host, user, password, db_name):
        self.host = host
        self.user = user
        self.password = password
        self.db_name = db_name


    def get_all_equip_names(self):
        
        logger.debug("connect to mysql database %s at host %s with user %s", self.db_name, self.host, self.user)
           
        cnx = mysql.connector.connect(user=self.user, password=self.password,
                              host=self.host,
                              database=self.db_name)
        cursor = cnx.cursor()

        query = ("SELECT id, name FROM energymanage_electricity_circuit")


        cursor.execute(query)

        names = []
        for (id,name) in cursor:
            index_map = {'id':id, 'name':name}
            names.append(index_map)
        cursor.close()
        cnx.close()
        return names

    def insert_energy_point_data(self):
        pass
        

class EquipEnergyData(object):
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
        self.quatity = None
        self.power = None 
    def __str__(self):
        return str(self.__dict__)


class OracleAdapter(object):

    def __init__(self, host, port, orcl_inst, user, password):
        self.host = host
        self.port = port
        self.orcl_inst = orcl_inst
        self.user = user
        self.password = password
    def __init__(self):
        self.user = os.environ['ORACLE_USER']
        self.password = os.environ['ORACLE_PASSWORD']
        self.host=os.environ['ORACLE_HOST']
        self.port=os.environ['ORACLE_PORT']
        self.orcl_inst=os.environ['ORACLE_INSTANCE']

        # select chinese
        os.environ["NLS_LANG"] = "American_America.ZHS16GBK"
        # where chinese
        os.environ["NLS_LANG"] = "AMERICAN_AMERICA.UTF8"
        # Connect as user "hr" with password "welcome" to the "oraclepdb" service running on this computer.
        self.connection = cx_Oracle.connect(self.user, self.password, "%s:%s/%s" % (self.host, self.port, self.orcl_inst))

    def _exe_sql(self, cursor, sql):
        print '#'*100
        print sql
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
        print '**' * 50
        equip_data = EquipEnergyData()
        equip_data.mysql_equip_id = mysql_equip_id
        equip_data.name = name
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
        print '#'* 50
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
        sql = """SELECT EQUIP_ID, EQUIP_TYPE_ID FROM hqliss1.EQ_EQUIP WHERE equip_name = '%s'""" % name


        try:
            cursor = self.connection.cursor()
            i = 0
            cursor.execute(sql)
            result = {}
            for x in cursor:
                i += 1
                print x
                result ={'id':x[0], 'type':x[1]}
            assert i == 1
            return result
        finally:
            cursor.close()

    def clear(self):
        self.connection.close()


def collect():
    logger.debug('Start to collect enegery data')
    mysql_host= os.environ['MYSQL_HOST']
    mysql_user = os.environ['MYSQL_USER']
    mysql_password = os.environ['MYSQL_PASSWORD']
    mysql_database = os.environ['MYSQL_DATABASE']
    mysqladapter = MySqlAdatper(mysql_host, mysql_user, mysql_password, mysql_database)
    indexes = mysqladapter.get_all_equip_names()
    logger.info("Equipments count: %s", len(indexes))

    oracle_adapter = OracleAdapter()
    for index in indexes[:10]:
        logger.debug('Collect data for equipment:%s', index)
        equip_energy_data = oracle_adapter.get_data_for_equip(index['name'], index['id'])
        print equip_energy_data
    oracle_adapter.clear()

if __name__ == '__main__':
    collect()


