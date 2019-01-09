# kafka-connectors

## 1.关于
  kafka-connectors 是基于 kafka 所提供 connect 组件所实现的 connector 插件。通过扩展该插件实现将
  kafka 的数据同步至 ClickHouse 中。在当前版本中，支持解析由 Debezium MySQL 插件采集的 binlog 数据和 Json 数据同步至 ClickHouse。

  关于 kafka connect 参见[传送门](https://kafka.apache.org/documentation/#connect "Kafka 官网")。
   
## 2.mysql-sink-clickhouse插件
  该插件用于解析由 [Debezium MySQL](https://github.com/debezium/debezium "debezium github") 采集的
  binlog 数据，通过 jdbc 同步至 ClickHouse 中。
  ### 2.1.connector参数配置：  
   | 参数名  | 默认值 | 是否必填 | 参数描述 |
   |-------- | :------:|:------:| ------ |
   | topics |          |   是   | 消费 kafka 的topic，多个以逗号分隔 |
   | clickhouse.hosts|    | 是 | ClickHouse 主机名，多个以逗号分隔 |
   | clickhouse.jdbc.port| | 是 | ClickHouse 端口号|
   | clickhouse.jdbc.user| | 否 | ClickHouse 用户名|
   | clickhouse.jdbc.password | | 否 | ClickHouse 密码|
   | clickhouse.sink.database| | 是 | 同步至 ClickHouse 的数据库名 |
   | clickhouse.sink.tables | 默认为解析的表名 | 否 | 同步至 ClickHouse 的分布式表名，多个表以逗号分隔，个数应与 topic 一致|
   | clickhouse.sink.local.tables | 默认为分布式表名加上 _local | 否 | 同步至 ClickHouse 的本地表名，多个表以逗号分隔，个数应与 topic 一致|
   | clickhouse.sink.date.columns | | 是 | 同步至 ClickHouse 的表的时间分区字段，个数应与 topic 一致|
   | clickhouse.source.date.columns | 默认取当前时间 | 否 | 指定写至ClickHouse分区字段的值的取值的字段，该值应存在在所选输入数据的topic的schema中，个数应与 topic 一致|
   | clickhouse.source.date.format | yyyy-MM-dd HH:mm:ss | 否| 指定clickhouse.source.date.columns改字段的日期format方式，个数应与 topic 一致|
   | clickhouse.optimize | true | 否 | 是否需要执行optimize本地表 |
  注：ClickHouse 在实践中需手动建表，不支持自动建表，在实践中一般使用 MergeTree 系列引擎加 Distributed 引擎。
   
## 3.json-sink-clickhouse插件
  该插件用于解析普通的 Json 格式的数据，通过 jdbc 同步至 ClickHouse 中。  
  ### 3.1.connector参数配置：  
  | 参数名  | 默认值 | 是否必填 | 参数描述 |
  |-------- | :------:|:------:| ------ |
  | topics |          |   是   | 消费 kafka 的topic，多个以逗号分隔 |
  | clickhouse.hosts|    | 是 | ClickHouse 主机名，多个以逗号分隔 |
  | clickhouse.jdbc.port| | 是 | ClickHouse 端口号|
  | clickhouse.jdbc.user| | 否 | ClickHouse 用户名|
  | clickhouse.jdbc.password | | 否 | ClickHouse 密码|
  | clickhouse.sink.database| | 是 | 同步至 ClickHouse 的数据库名 |
  | clickhouse.sink.tables | | 是 | 同步至 ClickHouse 的分布式表名，多个表以逗号分隔，个数应与 topic 一致|
  | clickhouse.sink.local.tables | 默认为分布式表名加上 _local | 否 | 同步至 ClickHouse 的本地表名，多个表以逗号分隔，个数应与 topic 一致|
  | clickhouse.sink.date.columns | | 是 | 同步至 ClickHouse 的表的时间分区字段，个数应与 topic 一致|
  | clickhouse.source.date.columns | 默认取当前时间 | 否 | 指定写至ClickHouse分区字段的值的取值的字段，该值应存在在所选输入数据的topic的schema中，个数应与 topic 一致|
  | clickhouse.source.date.format | yyyy-MM-dd HH:mm:ss | 否| 指定clickhouse.source.date.columns改字段的日期format方式，个数应与 topic 一致|
  | clickhouse.optimize | false | 否 | 是否需要执行optimize本地表 |
  注：ClickHouse 在实践中需手动建表，不支持自动建表，在实践中一般使用 MergeTree 系列引擎加 Distributed 引擎。

## 4.connector-test模块
  该模块适用于在开发完成之后单元测试完毕，然后进行联调测试 debug。在 [StandaloneTest](connector-test/src/test/java/com/rrc/bigdata/StandaloneTest.java) 类中，使用 Kafka connect 提供的 
  Standalone 默认，在本地启动一个 standalone 服务，通过这种方式能在本地 debug 调试所写的代码是否正确，提升插件的开发效率和插件的准确性。
  ### 4.1.测试步骤
  1. 修改配置文件  
  修改配置文件 [connect-standalone.properties](connector-test/src/test/resources/connect-standalone.properties) 中的参数 bootstrap.servers 
  为联调测试的 Kafka 地址。  
  修改或新增 Kafka connector 配置文件，[json-sink-clickhouse.properties](connector-test/src/test/resources/json-sink-clickhouse.properties)。
  2. 运行 StandaloneTest 类的 runTest 方法
  3. 测试 debug
  
