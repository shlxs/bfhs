bfhs
====

hive spi

正式环境部署下hive-jms-handler
##hive-jms-handler部署
* 将附件中的5个jar放到%HIVE_HOME%/lib，并添加classpath
* 将jms-site.xml放到%HIVE_HOME%/conf
* 修改jms-site.xml，将borker.url替换为正式bfac使用到的ActiveMQ地址

----------------------------------------------------------------------------
##hive-jms-handler使用规范
其中**加粗**部分不能修改，[…]部分可选

* 新建bitmap标记表，只需一张，帮助指定当前操作的bitmap表所属partition
<br />
CREATE TABLE bm_mark_table_name (bm_data_table_name string, base_time string) 
<br />
STORED BY 'com.bianfeng.bfas.hive.jms.JMSStorageHandler'
<br />
WITH SERDEPROPERTIES (
<br />
"message.conveter" = "com.bianfeng.bfas.hive.jms.converter.BaseTimeMarkConverter"
<br />
);

* 新建bitmap内容表，每种数据集一张表
<br />
CREATE TABLE bm_data_table_name (base_time string, data_col1 string[, data_col2, …]) 
<br />
STORED BY 'com.bianfeng.bfas.hive.jms.JMSStorageHandler'
<br />
WITH SERDEPROPERTIES (
<br />
"message.conveter" = "com.bianfeng.bfas.hive.jms.converter.MultiRecordsConverter"
<br />
[ ,"message.channel" = "other channel" ]
<br />
[ ,"message.buffer.size" = "10000" ]
<br />
[ ,"transaction.enable" = "true" ]
<br />
);

##参数说明
<table>
    <tr>
        <td>参数名</td>
        <td>说明</td>
        <td>默认值</td>
    </tr>
    <tr>
        <td>message.conveter</td>
        <td>指定Hive数据集到Message转换器</td>
        <td>无，必填</td>
    </tr>
    <tr>
        <td>message.channel</td>
        <td>指定ActiveMQ通道地址</td>
        <td>取决于message.conveter的设置, BaseTimeMarkConverter为数据项第一列，MultiRecordsConverter为hive表名</td>
    </tr>
    <tr>
        <td>message.buffer.size</td>
        <td>指定Hive数据集缓冲大小，缓冲后的数据将作为一个Message传送到ActiveMQ中</td>
        <td>取决于message.conveter的设置, BaseTimeMarkConverter为0，MultiRecordsConverter为100000</td>
    </tr>
    <tr>
        <td>transaction.enable</td>
        <td>是否启用ActiveMQ事务</td>
        <td>false</td>
    </tr>
</table>
<br />
##范例
```javascript
hive> CREATE TABLE bitmap_mark (bitmap_table_name string, base_time string) 
STORED BY 'com.bianfeng.bfas.hive.jms.JMSStorageHandler'
WITH SERDEPROPERTIES (
"message.conveter" = "com.bianfeng.bfas.hive.jms.converter.BaseTimeMarkConverter"
);
hive> CREATE TABLE pc_login_bitmap (base_time string, occur_time string, popt_id string, bfnum_id string) 
STORED BY 'com.bianfeng.bfas.hive.jms.JMSStorageHandler'
WITH SERDEPROPERTIES (
"message.conveter" = "com.bianfeng.bfas.hive.jms.converter.MultiRecordsConverter"
,"message.buffer.size" = "1000"
);
hive> insert overwrite table bitmap_mark select 'pc_login_bitmap','2014-02-12 08:00:00' from test2 limit 1;
hive> insert overwrite table pc_login_bitmap select '2014-02-12 08:00:00', '2014-02-12 08:22:00',id,id from test2;
```

