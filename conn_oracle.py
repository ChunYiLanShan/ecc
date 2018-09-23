# -*- coding: utf-8 -*-

import cx_Oracle
import sys
import os

oracle_user = os.environ['ORACLE_USER']
oracle_password = os.environ['ORACLE_PASSWORD']
oracle_host=os.environ['ORACLE_HOST']
oracle_port=os.environ['ORACLE_PORT']
oracle_instance=os.environ['ORACLE_INSTANCE']
# select chinese
os.environ["NLS_LANG"] = "American_America.ZHS16GBK"
# where chinese
os.environ["NLS_LANG"] = "AMERICAN_AMERICA.UTF8"

  
# Connect as user "hr" with password "welcome" to the "oraclepdb" service running on this computer.
connection = cx_Oracle.connect(oracle_user, oracle_password, "%s:%s/%s" % (oracle_host, oracle_port, oracle_instance))

cursor = connection.cursor()
def exe_sql(sql):
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

exe_sql('SELECT EQUIP_TYPE_ID, EQUIP_TYPE_NAME, DEPICT FROM hqliss1.EQ_EQUIP_TYPE where ROWNUM < 10');
exe_sql('SELECT point_id, point_name, short_code, depict,equip_no FROM hqliss1.RTM_POINT ORDER BY point_id');
exe_sql('SELECT EQUIP_ID, EQUIP_NAME, ASSET_CODE, EQUIP_TYPE_ID, POWER_TYPE FROM hqliss1.EQ_EQUIP ');
exe_sql('SELECT EQUIP_ID, EQUIP_NAME, ASSET_CODE, EQUIP_TYPE_ID, POWER_TYPE FROM hqliss1.EQ_EQUIP WHERE EQUIP_ID = 21294');
exe_sql('SELECT * FROM hqliss1.EQ_EQUIP_TYPE WHERE ROWNUM < 1000');
exe_sql('SELECT * FROM hqliss1.RTM_CONTROLDATA');
exe_sql("""SELECT equip_name, EQUIP_ID, EQUIP_TYPE_ID FROM hqliss1.EQ_EQUIP WHERE EQUIP_ID = 21719""")
exe_sql("""SELECT EQUIP_ID, EQUIP_TYPE_ID FROM hqliss1.EQ_EQUIP WHERE equip_name = '门诊楼B1层低配间A1L31柜螺杆机3号PE410R'""")

connection.close()
