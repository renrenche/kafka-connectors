package com.rrc.bigdata.connector;

import com.rrc.bigdata.clickhouse.ClickHouseConfigDef;
import com.rrc.bigdata.jdbc.JdbcDataSource;
import org.apache.kafka.common.config.Config;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigValue;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.sink.SinkConnector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;
import java.util.*;

import static com.rrc.bigdata.constant.CommonConstant.*;

/**
 * @author L
 */
public class MySqlSinkClickHouseConnector extends SinkConnector {
    private static final Logger logger = LoggerFactory.getLogger(MySqlSinkClickHouseConnector.class);
    private Map<String, String> props;

    @Override
    public void start(Map<String, String> props) {
        for (Map.Entry<String, String> entry : props.entrySet()) {
            logger.info("输入参数key: {}, value: {}", entry.getKey(), entry.getValue());
        }
        this.props = props;
    }

    @Override
    public Class<? extends Task> taskClass() {
        return MySqlSinkClickHouseTask.class;
    }

    @Override
    public List<Map<String, String>> taskConfigs(int maxTasks) {
        List<Map<String, String>> configs = new ArrayList<>();
        for (int i = 0; i < maxTasks; i++) {
            configs.add(props);
        }
        return configs;
    }

    @Override
    public void stop() {
        logger.info("停止Connector");
    }

    @Override
    public Config validate(Map<String, String> connectorConfigs) {
        ConfigDef configDef = config();
        Map<String, ConfigValue> configValues = ClickHouseConfigDef.emptyValidate(connectorConfigs, configDef);
        JdbcDataSource dataSource = ClickHouseConfigDef.clickHouseConnectValidate(configValues);
        tableValidate(dataSource, connectorConfigs, configValues);
        if (dataSource != null) {
            try {
                dataSource.close();
            } catch (SQLException e) {
                logger.error("关闭连接失败: ", e);
            }
        }
        return new Config(new LinkedList<>(configValues.values()));
    }

    @Override
    public ConfigDef config() {
        ConfigDef config = new ConfigDef();
        ClickHouseConfigDef.clickHouseConfigDef(config);
        config.define(CLICKHOUSE_SINK_TABLES, ConfigDef.Type.STRING, "",
                ConfigDef.Importance.LOW, "写到ClickHouse的分布式表名, 多个以逗号分隔" +
                        " 1.当此参数为空时, 所有topic数据都将写至对应MySQL的表名中; " +
                        " 2.当此参数不为空时, 其值应与topic个数一一对应, 对应topic将写至对应的表中");
        config.define(CLICKHOUSE_SINK_LOCAL_TABLES, ConfigDef.Type.STRING, "",
                ConfigDef.Importance.LOW, "写到ClickHouse的本地表名, 多个以逗号分隔" +
                        " 1.当此参数为空时, 将默认认为本地表名为分布式表加_local, 如: 分布式表为hello_ck, 则默认认为本地表名为hello_ck_local" +
                        " 2.当此参数不为空时, 其值应与ClickHouse对应分布式表一一对应. 即: 当clickhouse.sink.tables为空时, 需要与topic个数一一对应, " +
                        "   当clickhouse.sink.tables不为空时, 需要与clickhouse.sink.tables的参数一一对应. ");
        ClickHouseConfigDef.clickHousePartitionConfigDef(config);
        config.define(CLICKHOUSE_OPTIMIZE, ConfigDef.Type.STRING,
                ConfigDef.Importance.LOW, "是否需要执行optimize本地表, 可为空, 默认为true");
        logger.info("Connector 参数定义");
        return config;
    }

    @Override
    public String version() {
        return "1.0";
    }

    /**
     * 对输入的分布式表和本地表参数进行校验
     * 包含: 分布式表和本地表的个数, 分布式表和本地表是否存在ClickHouse中
     */
    private void tableValidate(JdbcDataSource dataSource, Map<String, String> connectorConfigs, Map<String, ConfigValue> configValues) {
        ConfigValue sinkTablesVal = configValues.get(CLICKHOUSE_SINK_TABLES);
        String topics = connectorConfigs.get(TOPICS);
        ConfigValue sinkLocalTablesVal = configValues.get(CLICKHOUSE_SINK_LOCAL_TABLES);
        clusterTableValidate(sinkTablesVal, topics);
        localTableValidate(sinkTablesVal, topics, sinkLocalTablesVal);
        String sinkDb = connectorConfigs.get(CLICKHOUSE_SINK_DATABASE);
        ClickHouseConfigDef.clusterTableExist(dataSource, sinkTablesVal, sinkDb);
        ClickHouseConfigDef.localTableExist(dataSource, sinkLocalTablesVal, sinkDb);
        ClickHouseConfigDef.validateSinkDateColumns(configValues, dataSource, sinkDb, sinkTablesVal);
        ClickHouseConfigDef.validateSourceDateColumns(configValues, dataSource, sinkTablesVal);
        ClickHouseConfigDef.validateDateFormat(configValues, dataSource, sinkTablesVal);
    }

    /**
     * 对输入的本地表进行校验
     * 如果输入不为空：
     * *   如果输入的分布式表不为空, 则判断其是否与分布式表一一对应
     * *   如果输入的分布式表为空, 则判断其是否与topic个数一一对应
     */
    private void localTableValidate(ConfigValue sinkTablesVal, String topics, ConfigValue sinkLocalTablesVal) {
        if (sinkLocalTablesVal.value() != null) {
            String sinkLocalTables = (String) sinkLocalTablesVal.value();
            String[] sinkLocalTabArr = sinkLocalTables.split(CONFIG_SEPARATOR);
            if (sinkTablesVal.value() != null) {
                String sinkTables = (String) sinkTablesVal.value();
                if (sinkLocalTabArr.length != sinkTables.split(CONFIG_SEPARATOR).length) {
                    sinkLocalTablesVal.addErrorMessage(sinkLocalTablesVal.name() + "需要与" + sinkTablesVal.name() + "一一对应");
                }
            } else {
                String[] topicsArr = topics.split(CONFIG_SEPARATOR);
                if (topicsArr.length != sinkLocalTabArr.length) {
                    sinkLocalTablesVal.addErrorMessage(sinkLocalTablesVal.name() + "需要与topic个数一一对应");
                }
            }
        }
    }

    /**
     * 校验输入的分布式表参数
     * 如果其值不为空, 则判断其个数是否为一个或者其个数是否与topic个数一一对应
     */
    private void clusterTableValidate(ConfigValue sinkTablesVal, String topics) {
        if (sinkTablesVal.value() != null) {
            String sinkTables = (String) sinkTablesVal.value();
            String[] sinkTabArr = sinkTables.split(CONFIG_SEPARATOR);
            String[] topicsArr = topics.split(CONFIG_SEPARATOR);
            if (topicsArr.length != sinkTabArr.length) {
                sinkTablesVal.addErrorMessage(sinkTablesVal.name() + "需要与topic个数一一对应");
            }
        }
    }

}
