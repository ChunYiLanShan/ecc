# ecc

# Run
* `docker run -e ECC_DURATION=1800 -dti ecc`

# Entity Model
* 一个设备会有多个采集点。所以，设备表(EQ_EQUIP)和点表(RTM_POINT)的关系是一对多的关系。
* 各表简介
    * EQ_EQUIP: 设备，如电表，水表。
    * RTM_POINT: 采集点
    * RTM_CONTROLDATA: 采集点的数据值

# Handle Data Lost
* Case: Database lost connection. 需要报警，发邮件到管理员？
* Case: Database connection is OK. But can't fetch data.
* Notes:
    * 使用EquipEnergyData类作为解耦的媒介，取来的数据放在这个里面，可能需要加一些字段（采集时间），在将这种类型的数据列表插入到
    * MySQL的时候，检查一下是否为更新/有效数据，如果不是，就估计一个值，继续更新这个数据类型的实例，那么一些函数(insert_energy_point_data_in_batch)就可以保持不动，
    * 遵守了开闭原则，保证了软件稳定性。
    * 这个逻辑可以在函数collect_electricity中，639行之后。
    * 需要标记哪些数据是估计的数据。在mysql中，是否要加字段。
    * 实现一个函数，把某个设备的某个采集点的数据，依据历史数据算出一个估计值。
        * 这个函数仅依赖于mysql
        * 算法可以根据算出一个增长值？ 不同的数据，处理方式会有不同吗？
    * 什么时候调用上面这个逻辑？
        * 数据库连不上时，对所有的设备执行这个操作。
        * 某个设备采集点在采集时，发现数据没有及时更新？
            * RTM_CONTROLDATA是不是也包含采集时间？
            * 原有系统是如何更新数据库？间隔是多少？或者不管间隔是多少，直接判断这个值已经被采集过，如果已被采集，就需要用估计值。
 * 金字塔的结构解释我们要做的改动。
 
 ### 处理丢失耗电数据的拟合方案设计
 * 前些时间，在新华医院的系统中，能耗的展示总是出现问题：耗电量突然变成零。这个问题的原因一般有两个，一个是Oracle数据库连不上；第二个是
 原有系统没有更新采集点数据到Oracle数据库。为了解决这个问题以提供更好的用户体验，我们决定使用拟合的方式填补这些没能收集到的数据。我们姑且
 把这个解决方法称之为耗电数据拟合方案。下面是对这个方案的范围，核心算法设计，算法正确性评估，算法复杂度分析，待讨论问题的详细介绍。
 * 这个拟合方案的目标范围是仅限于耗电量。
 * 这个方案的算法总体设计: 如果没搜集到最新数据，使用前七次搜集到的数据进行拟合，拟合的最新数据=最新的已收集数据 + 六次耗电量增加值的平均值。
 对应的Python伪代码：
 
 ```python
         
def collect_data_to_mysql(point_id):
    collected_data = oralce_db.get_data(point_id)
    is_data_changed = mysql_db.get_data(point_id) != collected_data
    history_count = 7
    if not is_data_changed:
        # 拟合数据 
        imported_data_list = mysql_db.get_data(point_id, history_count)
        delta = [imported_data_list[i]-imported_data_list[i-1] for i in range(1,len(imported_data_list))]
        mean_delta = sum(delta)/len(delta)
        collected_data = imported_data_list[-1] + mean_delta
    return collected_data      
```

 * 算法正确性评估
    * 如果采集点确实没变化呢？我以为这种的可能性很小，可以忽略。
    
 * 算法复杂度和性能评估：
    * 可能需要拟合依赖的历史数据，一次性从MySQL中加载到内存，不然的话，每次都要加载，就会拖慢速度。这个查询应该不会太慢。
    * 基于上一步，假如Oracle中的数据都不是最新的，若采集点的数量是n, 拟合历史数据个数是C(上面我们选择了7)，算法基本上可以控制在O(C*n)

 * 在这个方案中，还有一些需要讨论的问题
    * 是否要在MySQL数据库中加一个字段，表明这条数据是拟合得来的，还是真实的数据。
 

# Notes:
* You have to prepare instantclient\_18\_3
