# -*- coding: utf-8 -*-

from datetime import datetime
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
        self.db_conn = mysql.connector.connect(user=self.user, password=self.password,
                              host=self.host,
                              database=self.db_name)



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
    def clear(self):
        self.db_conn.close()
        

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

        #mapping data
        self.point_id_to_type = {} 

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

    def get_equip_name_to_ids(self, names):
        '''
        return {'name':'id', ...}
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
                print row_dict
                result[row_dict['EQUIP_NAME']] = '%s.%s' % (row_dict['EQUIP_TYPE_ID'], row_dict['EQUIP_ID'])
            if len(names) > len(result):
                logger.warn('Not found some corresponding equip based on name')
            return result
        finally:
            cursor.close()

    def get_point_id_type(equip_id_list):
        '''
        return {'equip_id': {'point_id':'point_type', ... }, ... }
        '''

        equip_no_list = ','.join(map(lambda e : "'%s'" % e, equip_id_list))

        sql = """SELECT point_id, point_name, short_code, depict, equip_no FROM hqliss1.RTM_POINT WHERE equip_no IN (%s)""" % equip_no_list 
        logger.info('SQL for get_point_id_type: %s', sql)

        try:
            cursor = self.connection.cursor()
            i = 0
            cursor.execute(sql)
            result = {}
            rows_list = self._rows_to_dict_list(cursor)

            return result
        finally:
            cursor.close()

    def get_point_id_to_value(point_id_list):
        '''
        return {'point_id':'value', ... }
        '''
        pass

    def clear(self):
        self.connection.close()


def get_equip_engery_data_in_batch(oralce_adapter, equip_energy_data_list):
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
    for k, v in name_to_energy_data.items():
        v.oracle_equip_id = name_to_ids[k]

    # Get point_id -> point_type
    oracle_equip_id_to_point_id_type = oracle_adapter.get_point_id_type([e.oracle_equip_id for e in eedl])
    oracle_equip_id_to_energy_data = dict(zip([e.oracle_equip_id for e in eedl], eedl))
    for oracle_equip_id, energy_data in oracle_equip_id_to_energy_data:
        energy_data.point_id_to_type = oracle_equip_id_to_point_id_type[oracle_equip_id]

    # Get point value and update to energy data
    point_id_list = []
    for k,v in oracle_equip_id_to_point_id_type:
        point_id_list += v.keys()

    point_id_to_value = oracle_adapter.get_point_id_to_value(point_id_list)
    point_id_to_energy_data = {}
    for e in eedl:
        point_id_to_type = e.point_id_to_type
        for point_id in point_id_to_type.keys():
            point_id_to_energy_data[point_id] = e

    for point_id, point_value in point_id_to_value:
        e = point_id_to_energy_data[point_id]
        setattr(e, e.point_id_to_type, point_value)



def collect():
    logger.debug('Start to collect enegery data')
    mysql_host= os.environ['MYSQL_HOST']
    mysql_user = os.environ['MYSQL_USER']
    mysql_password = os.environ['MYSQL_PASSWORD']
    mysql_database = os.environ['MYSQL_DATABASE']
    mysqladapter = MySqlAdatper(mysql_host, mysql_user, mysql_password, mysql_database)
    indexes = mysqladapter.get_all_equip_names()

    #in batch style
    equip_energy_data_list = []
    for id_type_pair in indexes[:10]:
        equip_energy_data = EquipEnergyData()
        self.mysql_equip_id = id_type_pair['id']
        self.name = id_type_pair['name']
        equip_energy_data_list.append(equip_energy_data)


    logger.info("Equipments count: %s", len(indexes))

    oracle_adapter = OracleAdapter()
    get_equip_engery_data_in_batch(oracle_adapter, equip_energy_data_list)
    for index in indexes[:100]:
        logger.debug('Collect data for equipment:%s', index)
        equip_energy_data = oracle_adapter.get_data_for_equip(index['name'], index['id'])
        print equip_energy_data
        mysqladapter.insert_energy_point_data(equip_energy_data)
    oracle_adapter.clear()
    mysqladapter.clear()

def test_get_equip_name_to_ids():
    oracle_adapter = OracleAdapter()
    names = [u'门诊楼B1层低配间A1L31柜螺杆机3号PE410R', u'科教楼B1层低配间行政楼空调PE410R']
    result = oracle_adapter.get_equip_name_to_ids(names)
    print '##'*50
    print result
    oracle_adapter.clear()
if __name__ == '__main__':
    test_get_equip_name_to_ids()


