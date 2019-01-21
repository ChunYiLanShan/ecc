# -*- coding: utf-8 -*-

import os
import mysql.connector

mysql_host= os.environ['MYSQL_HOST']
mysql_user = os.environ['MYSQL_USER']
mysql_password = os.environ['MYSQL_PASSWORD']
mysql_database = os.environ['MYSQL_DATABASE']
cnx = mysql.connector.connect(user=mysql_user, password=mysql_password,
                              host=mysql_host,
                              database=mysql_database)
cursor = cnx.cursor()

query = ("SELECT name FROM energymanage_electricity_circuit")


cursor.execute(query)

for x in cursor:
    for y in x:
        print y
cursor.close()
cnx.close()
