# -*- coding: utf-8 -*-

import datetime
import mysql.connector
import logging
import os
import sys

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

    def get_data_for_equip(self,name):
        pass
        import cx_Oracle

        # select chinese
        os.environ["NLS_LANG"] = "American_America.ZHS16GBK"
        # where chinese
        os.environ["NLS_LANG"] = "AMERICAN_AMERICA.UTF8"

          
        # Connect as user "hr" with password "welcome" to the "oraclepdb" service running on this computer.
        connection = cx_Oracle.connect(self.user, self.password, "%s:%s/%s" % (self.host, self.port, self.orcl_inst))

        sql = """SELECT EQUIP_ID, EQUIP_TYPE_ID FROM hqliss1.EQ_EQUIP WHERE equip_name = '%s'""" % name

        cursor = connection.cursor()
        self._exe_sql(cursor, sql)
        connection.close()


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
        oracle_adapter.get_data_for_equip(index['name'])



    

if __name__ == '__main__':
    collect()


