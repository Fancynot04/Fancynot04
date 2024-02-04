/*
Presto概述:
    Presto是FaceBook公司推出的分布式查询引擎, 也是主从架构, 用来解决Hive执行速度慢的问题的, 是一种 纯内存计算框架.
Presto架构:
    MS架构(主从架构), 主节点负责解析SQL, 生成执行计划.  从节点负责执行具体的执行计划.
Presto的安装:
    1. 两台机器都要先安装JDK, 装完后测试下.
        java -version
        jps                 -- 这个命令可以用了.
    2. 去hadoop01机器中创建 /export/server 目录, 然后把 presto的安装包上传到这里, 解压, 改名.
    3. 创建 /export/server/presto/etc目录, 用于配置presto的配置文件.
    4. 修改presto的配置文件, 一共4个, 分别是:
        /export/server/presto/etc/config.properties
        /export/server/presto/etc/jvm.config
        /export/server/presto/etc/node.properties
        /export/server/presto/etc/catalog/hive.properties
    5. scp拷贝给hadoop02, 然后修改2个配置文件.
        /export/server/presto/etc/config.properties
        /export/server/presto/etc/node.properties
Presto的优化:
    角度1: SQL常规优化.
        1. 列裁剪
            能写 select 列1, 列2 from 表名, 就不要写 select * from 表名
        2. 分区裁剪
            select 列1, 列2.. from 分区表 where 分区字段...      如果是分区表, 查询时记得写分区条件.
        3. group by优化
            如果是多字段分组, 则分组时按照数据量大小降序排列, 例如: 不要写 group by gender, id  而是写 group by id, gender, 因为id比gender(去重后个数)多
        4. order by使用limit
            一方面可以防止将所有数据加载到内存, 导致内存溢出的情况, 另一方面可以提高效率. 类似于: 想要排名年级前5, 首先要做到班级前5.
        5. 用regexp_like代替多个like语句
        6. join时候大表放置在左边
            大表 join 小表, 而不是  小表 join 大表, 因为Presto的底层会默认把左边的表拆分成N份, 然后依次和右边的表 join连接查询.
        7. 替换非ORC格式的Hive表
            建议创建Hive表的时候, 使用 orc + snappy方式, 不要用默认的 行存储(TextFile).

    角度2: 内存优化.  我们只需要关心如下的3个问题即可.
        1. Presto的内存分类, 分为: user memory(用户内存)  和  系统内存(System Memory)
        2. 谁来管理内存.
            Presto通过内存池来管理内存, 它(内存池)又分为两种情况,
            常规内存池: General Pool, 优先用这里.
            预留内存池: Reserved Pool, 常规内存不够用了, 才会用这里.
        3. 和Presto的内存调优参数有哪些(了解).


*/

-- 1. 查看所有的数据表.
show tables;

-- 2. 查看表中的内容.
select * from yp_ods.t_brand;

-- 3. 测试Presto执行Hive的速度, 大概能提速 3 ~ 7倍.
select level, count(id) as total_cnt from yp_dwd.dim_goods_class group by level;        -- 1s

-- 4. 因为Presto可以连接多种数据源, 例如: MySQL, Hive等, 所以Presto中对于时间格式有特殊的要求, 必须是 年-月-日 时:分:秒 的格式.
-- Presto中和时间相关的4个函数如下:
-- 4.1 date_format(timestamp, format)  ==> varchar      作用: 将指定的 日期对象 转换为 字符串操作
-- 细节: 格式化的时候, 日期模板可以自定义
select date_format(timestamp '2023-05-30 13:14:21', '%Y/%m/%d %H:%i:%s');       -- 2023/05/30 13:14:21
select date_format(timestamp '2023/05/30 13:14:21', '%Y/%m/%d %H:%i:%s');       -- 报错, '2023/05/30 13:14:21' 不是Presto中的标准时间格式.

-- 4.2 date_parse(string, format) → timestamp       作用: 用于将字符串的日期数据转换为日期对象
-- 细节: 解析的时候, 日期模板必须和字符串格式保持一致.
select date_parse('2023年05月30日 13:14:21', '%Y年%m月%d日 %H:%i:%s');      -- 2023-05-30 13:14:21

-- 4.3 date_add(unit, value, timestamp) → [same as input]       作用: 用于对日期数据进行 加 减 操作
select date_add('year', 1, timestamp '2023-05-30 13:14:21');      -- 2024-05-30 13:14:21
select date_add('month', -2, timestamp '2023-05-30 13:14:21');      -- 2023-03-30 13:14:21

-- 4.4 date_diff(unit, timestamp1, timestamp2) → bigint     作用: 用于比对两个日期之间差值
select date_diff('year', timestamp '2023-05-30', timestamp '2025-05-30');       -- 2, 后 - 前     这里的timestamp的意思是 标记作用, 标记着后边是时间戳, 而不是字符串.

-- 使用Presto的1个小Bug, 通过Presto插入的数据(insert + select), 通过hive查询, 查询不到.
-- 今日任务(Day05):  1. 搭建完毕DWB层.    2.成功安装Presto, 并通过DataGrip连接, 测试.
