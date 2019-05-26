
# New

SELECT id,name from ec_device_info where name like '%门诊楼B1层低配间空调操作配电室%';

SELECT id, name from xh.ec_device_info where name = '门诊楼B1层低配间空调操作配电室1PE410R';
SELECT id,projectpoint,POINTDESC,name from ec_point_info where DEVICEINFO_ID = 'ff80808167ba81370167bb040f3e044f';
SELECT id,projectpoint from ec_point_info where projectpoint = '8001.208.1'
select projectpoint from ec_sdcd_data where projectpoint = '8001.208.1'
select projectpoint,record,recordtime,optime from xh.ec_sdcd_data where projectpoint = '8001.208.1'
select projectpoint,record,recordtime,optime from xh.ec_sdcd_data where projectpoint like '8001.208.%'


# 
453,儿外科B1层低配间一般冷冻机电力3PE410R
SELECT id, name from xh.ec_device_info where name like '%儿外科B1层低配间一般冷冻机电力3PE410R%';
SELECT id,projectpoint,POINTDESC,name from xh.ec_point_info where DEVICEINFO_ID = 'ff80808167ba81370167bb045590087b';
ff80808167ba81370167bb2a5d7773eb
8001.268.8
儿外科B1层低配间一般冷冻机电力3PE410R-A相电压
SELECT id,projectpoint from xh.ec_point_info where projectpoint = '8001.268.8';
ff80808167ba81370167bb2a5d7773eb
8001.268.8
select projectpoint,record,recordtime,optime from xh.ec_sdcd_data where projectpoint like '8001.268.%'
SELECT projectpoint, record FROM xh.ec_sdcd_data WHERE projectpoint IN ('8001.268.8');

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

#mysql
select count(*) from energymanage_device;

# Logic

mysql.energymanage_device.name: oralce.devie.name =>  oralce.ec_device_info.id: oralce.ec_point_info.device_id
=> list(ec_point_info[projectpoint, pointdesc]): EnergeyData => ec_point_info.projectpoint:ec_sdcd_data.projectpoint 
=> ec_sdcd_data.record (populate EnergeyData)

#Mapping

# eq_equip -> ec_device_info
* equip_name -> name

# RTM_POINT -> ec_point_info
* DPICT -> POINTDESC
