# ecc

# Run
* `docker run -e ECC_DURATION=1800 -dti ecc`

# Entity Model
* 一个设备会有多个采集点。所以，设备表(EQ_EQUIP)和点表(RTM_POINT)的关系是一对多的关系。
* 各表简介
    * EQ_EQUIP: 设备，如电表，水表。
    * RTM_POINT: 采集点
    * RTM_CONTROLDATA: 采集点的数据值

# Notes:
* You have to prepare instantclient\_18\_3
