package com.rrc.bigdata.connector;

import com.rrc.bigdata.clickhouse.ClickHouseConfigDef;
import com.rrc.bigdata.jdbc.JdbcDataSource;
import org.apache.commons.lang3.StringUtils;
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
public class JsonSinkClickHouseConnector extends SinkConnector {
    private static final Logger logger = LoggerFactory.getLogger(JsonSinkClickHouseConnector.class);
    private Map<String, String> props;

    @Override
    public String version() {
        return "1.0";
    }

    @Override
    public void start(Map<String, String> props) {
        for (Map.Entry<String, String> entry : props.entrySet()) {
            logger.info("输入参数key: {}, value: {}", entry.getKey(), entry.getValue());
        }
        this.props = props;
    }

    @Override
    public Class<? extends Task> taskClass() {
        return JsonSinkClickHouseTask.class;
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
    public ConfigDef config() {
        ConfigDef config = new ConfigDef();
        ClickHouseConfigDef.clickHouseConfigDef(config);
        config.define(CLICKHOUSE_SINK_TABLES, ConfigDef.Type.STRING,
                ConfigDef.Importance.HIGH, "写到ClickHouse的分布式表名, 多个以逗号分隔" +
                        "此参数不能为空, 且应与topic个数一一对应, 对应topic将写至对应的表中");
        config.define(CLICKHOUSE_SINK_LOCAL_TABLES, ConfigDef.Type.STRING, "",
                ConfigDef.Importance.LOW, "写到ClickHouse的本地表名, 多个以逗号分隔" +
                        " 1.当此参数为空时, 将默认认为本地表名为分布式表加_local, 如: 分布式表为hello_ck, 则默认认为本地表名为hello_ck_local" +
                        " 2.当此参数不为空时, 其值应与ClickHouse对应分布式表一一对应. 即: 需要与clickhouse.sink.tables的参数一一对应. ");
        ClickHouseConfigDef.clickHousePartitionConfigDef(config);
        config.define(CLICKHOUSE_OPTIMIZE, ConfigDef.Type.STRING,
                ConfigDef.Importance.LOW, "是否需要执行optimize本地表, 可为空, 默认为false");
        logger.info("Connector 参数定义");
        return config;
    }

    @Override
    public Config validate(Map<String, String> connectorConfigs) {
        ConfigDef configDef = config();
        Map<String, ConfigValue> configValues = ClickHouseConfigDef.emptyValidate(connectorConfigs, configDef);
        JdbcDataSource dataSource = ClickHouseConfigDef.clickHouseConnectValidate(configValues);
        String sinkDb = connectorConfigs.get(CLICKHOUSE_SINK_DATABASE);
        ConfigValue sinkTablesVal = configValues.get(CLICKHOUSE_SINK_TABLES);
        ConfigValue sinkLocalTablesVal = configValues.get(CLICKHOUSE_SINK_LOCAL_TABLES);
        String topics = connectorConfigs.get(TOPICS);

        validateTables(sinkTablesVal, sinkLocalTablesVal, topics);

        ClickHouseConfigDef.clusterTableExist(dataSource, sinkTablesVal, sinkDb);
        ClickHouseConfigDef.localTableExist(dataSource, sinkLocalTablesVal, sinkDb);
        ClickHouseConfigDef.validateSinkDateColumns(configValues, dataSource, sinkDb, sinkTablesVal);
        ClickHouseConfigDef.validateSourceDateColumns(configValues, dataSource, sinkTablesVal);
        ClickHouseConfigDef.validateDateFormat(configValues, dataSource, sinkTablesVal);

        if (dataSource != null) {
            try {
                dataSource.close();
            } catch (SQLException e) {
                logger.error("关闭连接失败: ", e);
            }
        }
        return new Config(new LinkedList<>(configValues.values()));
    }

    private void validateTables(ConfigValue sinkTablesVal, ConfigValue sinkLocalTablesVal, String topics) {
        if (sinkTablesVal.value() != null && StringUtils.isNotEmpty(topics)) {
            String sinkTables = (String) sinkTablesVal.value();
            String[] sinkTabArr = sinkTables.split(CONFIG_SEPARATOR);
            String[] topicsArr = topics.split(CONFIG_SEPARATOR);
            if (topicsArr.length != sinkTabArr.length) {
                sinkTablesVal.addErrorMessage(sinkTablesVal.name() + "需要与topic个数一一对应");
            }
            if (sinkLocalTablesVal.value() != null) {
                String[] sinkLocalTabArr = ((String) sinkLocalTablesVal.value()).split(CONFIG_SEPARATOR);
                if (topicsArr.length != sinkLocalTabArr.length) {
                    sinkLocalTablesVal.addErrorMessage(sinkLocalTablesVal.name() + "需要与topic个数一一对应");
                }
            }
        }
    }

}
