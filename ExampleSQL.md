> Select equip_id,equip_name,equip_code,EQUIP_NO from eq_equip where equip_name = '门诊楼B1层低配间空调操作配电室1PE410R'


  EQUIP_ID
----------
EQUIP_NAME
--------------------------------------------------------------------------------
EQUIP_CODE
--------------------------------------------------------------------------------
EQUIP_NO
--------------------------------------------------------------------------------
     20892
门诊楼B1层低配间空调操作配电室1PE410R
MZLDP1-PE410R-1
8001.20892




> SELECT POINT_ID,SHORT_CODE FROM rtm_point WHERE EQUIP_NO = '8001.20892'

SQL> SELECT POINT_ID,SHORT_CODE FROM rtm_point WHERE EQUIP_NO = '8001.20892';

POINT_ID
--------------------------------------------------------------------------------
SHORT_CODE
--------------------------------------------------------------------------------
310109.XH.8001.20892.2
20892.2

310109.XH.8001.20892.3
20892.3

310109.XH.8001.20892.5
20892.5



# New

SELECT id,name from ec_device_info where name like '%门诊楼B1层低配间空调操作配电室%';

SELECT id,name from ec_device_info where name = '门诊楼B1层低配间空调操作配电室1PE410R';
SELECT id,projectpoint,POINTDESC,name from ec_point_info where DEVICEINFO_ID = 'ff80808167ba81370167bb040f3e044f';
SELECT id,projectpoint from ec_point_info where projectpoint = '8001.208.1'
select projectpoint from ec_sdcd_data where projectpoint = '8001.208.1'
select projectpoint,record,recordtime,optime from ec_sdcd_data where projectpoint = '8001.208.1'

ID
--------------------------------------------------------------------------------
NAME
--------------------------------------------------------------------------------
ff80808167ba81370167bb03d9620087
门诊楼空调系统8层新风机


SELECT projectpoint,record FROM EC_SDCD_DATA where projectpoint = 'ff80808167ba81370167bb0498c10b87';
SELECT projectpoint,record FROM EC_SDCD_DATA where ROWNUM < 5;
## Relation

ec_point_info.DEVICEINFO_ID ---- n:1 ----->  ec_device_info.id
ec_point_info.projectpoint -- ?:? ----> ec_sdcd_data.projectpoint


# Logic

mysql.device.name: oralce.devie.name =>  oralce.ec_device_info.id: oralce.ec_point_info.device_id
=> list(ec_point_info[projectpoint, pointdesc]): EnergeyData => ec_point_info.projectpoint:ec_sdcd_data.projectpoint 
=> ec_sdcd_data.record (populate EnergeyData)

#Mapping

# eq_equip -> ec_device_info
* equip_name -> name

# RTM_POINT -> ec_point_info
* DPICT -> POINTDESC
