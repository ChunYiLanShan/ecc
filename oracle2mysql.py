# -*- coding: utf-8 -*-

import datetime
import mysql.connector
import logging
import os

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

        query = ("SELECT name FROM energymanage_electricity_circuit")


        cursor.execute(query)

        names = []
        for x in cursor:
            for y in x:
                names.append(y)
        cursor.close()
        cnx.close()
        return names
        

def mysql_example():
    host='180.180.180.183'
    cnx = mysql.connector.connect(user='root', password='zzkjyunwei',
                              host=host,
                              database='dfyyfm512')
    cursor = cnx.cursor()

    query = ("SELECT name FROM energymanage_electricity_circuit")


    cursor.execute(query)

    for x in cursor:
        for y in x:
            print y
    cursor.close()
    cnx.close()

def collect():
    logger.debug('Start to collect enegery data')
    mysql_host= os.environ['MYSQL_HOST']
    mysql_user = os.environ['MYSQL_USER']
    mysql_password = os.environ['MYSQL_PASSWORD']
    mysql_database = os.environ['MYSQL_DATABASE']
    mysqladapter = MySqlAdatper(mysql_host, mysql_user, mysql_password, mysql_database)
    names = mysqladapter.get_all_equip_names()
    for name in names:
        print name
    

if __name__ == '__main__':
    collect()


