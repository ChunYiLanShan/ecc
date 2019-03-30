# Oracle mapping between DB 2.0 and DB 2.5

# DB 2.0 Schema hqliss1
## DB2.0: RTM_CONTROLDATA

* Define

```text
SQL> describe hqliss1.RTM_CONTROLDATA
 Name					   Null?    Type
 ----------------------------------------- -------- ----------------------------
 POINT_ID				   NOT NULL NVARCHAR2(50)
 RECORD 					    FLOAT(126)
 RECORDTIME					    DATE
 OPTIME 					    DATE
```

* point_id 
    * Used: Yes.
    * Example: 310109.XH.7002001.3.2
* RECORD
    * Used: Yes.
    * Example: 

## DB2.0: hqliss1.EQ_EQUIP 对应 2.5的EC_DEVICE_INFO
* Define
```text
SQL> describe eq_equip
 Name					   Null?    Type
 ----------------------------------------- -------- ----------------------------
 EQUIP_ID				   NOT NULL NUMBER
 EQUIP_NAME					    NVARCHAR2(100)
 EQUIP_CODE					    NVARCHAR2(50)
 EQUIP_TYPE_ID				   NOT NULL NUMBER
 ASSET_CODE					    NVARCHAR2(50)
 EQUIP_QUALITY					    NVARCHAR2(50)
 BUILD_ID					    NUMBER
 STOREY 					    NUMBER
 PLACE						    NVARCHAR2(500)
 BRAND						    NVARCHAR2(50)
 MODEL						    NVARCHAR2(50)
 PRODUCTION					    NVARCHAR2(100)
 ORGIN						    NVARCHAR2(100)
 PRODUCT_DATE					    DATE
 INSTALL_DATE					    DATE
 USE_DATE					    DATE
 SERVICE_LIFE					    NUMBER(3)
 WARRANTY					    DATE
 PURCHASE					    NUMBER(12,2)
 SERVICE_CYCLE					    NUMBER
 ACCESSORY					    NVARCHAR2(100)
 POWER_TYPE					    NUMBER
 NODE						    NUMBER
 NODE_PARENT					    NUMBER
 NODE_TOP					    NUMBER
 STATUS 					    NVARCHAR2(50)
 MONITOR_STATUS 				    NUMBER(1)
 EQUIP_NO					    NVARCHAR2(50)
 REMARKS					    NVARCHAR2(1000)
 WARRANTY_FLAG					    NUMBER(1)
 SERVICE_LIFE_FLAG				    NUMBER(1)
 MOSA_NO					    NVARCHAR2(10)
 OPTIME 					    DATE
```

## DB2.0:RTM_POINT

* Define:
```text

SQL> describe rtm_point;
 Name					   Null?    Type
 ----------------------------------------- -------- ----------------------------
 POINT_ID				   NOT NULL NVARCHAR2(50)
 POINT_NAME					    NVARCHAR2(50)
 SHORT_CODE				   NOT NULL NVARCHAR2(50)
 DEPICT 					    NVARCHAR2(150)
 EQUIP_NO					    NVARCHAR2(50)
 UNIT						    NVARCHAR2(50)
 VALUE_UP					    FLOAT(126)
 VALUE_DOWN					    FLOAT(126)
 PROTOCOL					    NVARCHAR2(20)
 REFLASH					    NUMBER
 DELAY_TIME					    NUMBER
 REPEAT_TIMES					    NUMBER(3)
 SHOW_FLAG					    NUMBER(1)
 MAINTAIN_FLAG					    NUMBER(1)
 ALARM_FLAG					    NUMBER(1)
 ANALYSIS_FLAG					    NUMBER(1)
 THIRD_FLAG					    NUMBER(1)
 BOOL_0 					    NVARCHAR2(50)
 BOOL_1 					    NVARCHAR2(50)
 SHOW_DATA_FLAG 				    NUMBER(1)
 SHOW_STATUS_FLAG				    NUMBER(1)
 SHOW_TITLE					    NVARCHAR2(50)
 QUALITY					    NUMBER(1)
 THIRD						    NUMBER(1)
 OPTIME 				   NOT NULL DATE
 STATUS 					    NUMBER(1)
 CONTROLL_POINT 				    NVARCHAR2(100)


```
# DB 2.5: xh
## DB2.5: xh.ec_sdcd_data

```text
SQL> describe xh.ec_sdcd_data;
 Name					   Null?    Type
 ----------------------------------------- -------- ----------------------------
 PROJECTPOINT					    VARCHAR2(255)
 RECORD 					    FLOAT(126)
 RECORDTIME					    DATE
 OPTIME 					    DATE
```

* PROJECTPOINT:
    * Example: 8001.5.11

## DB2.5: EC_POINT_INFO
* Define

```text
SQL> Describe ec_point_info;
 Name					   Null?    Type
 ----------------------------------------- -------- ----------------------------
 ID					   NOT NULL VARCHAR2(32 CHAR)
 CREATETIME					    TIMESTAMP(6)
 MODIFYTIME					    TIMESTAMP(6)
 TIMESTAMP				   NOT NULL VARCHAR2(255 CHAR)
 NAME						    VARCHAR2(255 CHAR)
 PINYIN 					    VARCHAR2(255 CHAR)
 PINYINHEAD					    VARCHAR2(255 CHAR)
 STATE					   NOT NULL VARCHAR2(255 CHAR)
 PROCESSSTATE					    VARCHAR2(10 CHAR)
 CANCELREASION					    VARCHAR2(255 CHAR)
 COLLECTTYPE					    VARCHAR2(255 CHAR)
 DELAY						    NUMBER(10)
 OLDPROJECTPOINT				    VARCHAR2(255 CHAR)
 OPTIME 					    TIMESTAMP(6)
 POINTDESC					    VARCHAR2(255 CHAR)
 POINTTYPE					    VARCHAR2(255 CHAR)
 PROJECTPOINT					    VARCHAR2(255 CHAR)
 RECORD 					    NUMBER(19,2)
 THIRDPART					    NUMBER(10)
 TOUSE						    NUMBER(10)
 UNIT						    VARCHAR2(255 CHAR)
 COMPANY_ID					    VARCHAR2(32 CHAR)
 CREATER_ID					    VARCHAR2(32 CHAR)
 DEVICEINFO_ID					    VARCHAR2(32 CHAR)
 MODBUS_ID					    VARCHAR2(32 CHAR)
 OPC_ID 					    VARCHAR2(32 CHAR)
 POINTDEFINE_ID 				    VARCHAR2(32 CHAR)

```

## DB2.5: EC_DEVICE_INFO

* Define xh.EC_DEVICE_INFO

```text
SQL> describe xh.EC_DEVICE_INFO;
 Name					   Null?    Type
 ----------------------------------------- -------- ----------------------------
 ID					   NOT NULL VARCHAR2(32 CHAR)
 CREATETIME					    TIMESTAMP(6)
 MODIFYTIME					    TIMESTAMP(6)
 TIMESTAMP				   NOT NULL VARCHAR2(255 CHAR)
 NAME						    VARCHAR2(255 CHAR)
 PINYIN 					    VARCHAR2(255 CHAR)
 PINYINHEAD					    VARCHAR2(255 CHAR)
 STATE					   NOT NULL VARCHAR2(255 CHAR)
 PROCESSSTATE					    VARCHAR2(10 CHAR)
 ASSETNO					    VARCHAR2(255 CHAR)
 CREATEUNIT					    VARCHAR2(255 CHAR)
 DEVICEBRAND					    VARCHAR2(255 CHAR)
 DEVICEID					    VARCHAR2(255 CHAR)
 DEVICEIDNO				   NOT NULL NUMBER(10)
 DEVICEMODEL					    VARCHAR2(255 CHAR)
 DEVICENO					    VARCHAR2(255 CHAR)
 FIXNUM 					    NUMBER(21,2)
 FLOORFUN					    VARCHAR2(255 CHAR)
 INSTALLDATE					    TIMESTAMP(6)
 INSTALLPOSITION				    VARCHAR2(255 CHAR)
 ISMONITOR					    VARCHAR2(255 CHAR)
 MAINTAINCYCLE					    NUMBER(21,4)
 METERLEVEL					    VARCHAR2(255 CHAR)
 PRODUCEDATE					    TIMESTAMP(6)
 PRODUCEPLACE					    VARCHAR2(255 CHAR)
 PURCHASEPRICE					    NUMBER(21,4)
 QUALITYDATE					    TIMESTAMP(6)
 REMARK 					    VARCHAR2(255 CHAR)
 USESTARTDATE					    TIMESTAMP(6)
 USEYEAR					    NUMBER(10)
 COMPANY_ID					    VARCHAR2(32 CHAR)
 CREATER_ID					    VARCHAR2(32 CHAR)
 BUILDINFO_ID					    VARCHAR2(32 CHAR)
 DEVICEINFOVERSION_ID				    VARCHAR2(32 CHAR)
 DEVICENATURE_ID				    VARCHAR2(32 CHAR)
 DEVICESTATE_ID 				    VARCHAR2(32 CHAR)
 ENERGYTYPE_ID					    VARCHAR2(32 CHAR)
 EQUIPTYPE_ID					    VARCHAR2(32 CHAR)
 FORSYSTEM_ID					    VARCHAR2(32 CHAR)
 PARENT_ID					    VARCHAR2(32 CHAR)
```

* select id,name,deviceid,deviceidno,devicemodel,equiptype_id from xh.ec_device_info where rownum < 10;
select id,name,equiptype_id from xh.ec_device_info where rownum < 5

* count: 670