package com.rrc.bigdata.clickhouse;

import com.rrc.bigdata.jdbc.JdbcConnectConfig;
import com.rrc.bigdata.jdbc.JdbcDataSource;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.rrc.bigdata.constant.CommonConstant.*;
import static com.rrc.bigdata.constant.CommonConstant.CLICKHOUSE_JDBC_PASSWORD;
import static com.rrc.bigdata.constant.CommonConstant.CLICKHOUSE_SINK_DATABASE;

/**
 * @author L
 */
public class ClickHouseConfigDef {
    private static final Logger logger = LoggerFactory.getLogger(ClickHouseConfigDef.class);

    public static void clickHouseConfigDef(ConfigDef config) {
        config.define(TOPICS, ConfigDef.Type.STRING,
                ConfigDef.Importance.HIGH, "消费的Kafka topic列表，以逗号分隔");
        config.define(CLICKHOUSE_HOSTS, ConfigDef.Type.STRING,
                ConfigDef.Importance.HIGH, "连接clickhouse的host列表，以逗号分隔");
        config.define(CLICKHOUSE_JDBC_PORT, ConfigDef.Type.STRING,
                ConfigDef.Importance.HIGH, "连接clickhouse jdbc端口号");
        config.define(CLICKHOUSE_JDBC_USER, ConfigDef.Type.STRING, "",
                ConfigDef.Importance.LOW, "连接clickhouse jdbc user");
        config.define(CLICKHOUSE_JDBC_PASSWORD, ConfigDef.Type.STRING, "",
                ConfigDef.Importance.LOW, "连接clickhouse jdbc password");
        config.define(CLICKHOUSE_SINK_DATABASE, ConfigDef.Type.STRING,
                ConfigDef.Importance.HIGH, "sink 至clickhouse 的数据库");
    }

    public static void clickHousePartitionConfigDef(ConfigDef config) {
        config.define(CLICKHOUSE_SINK_DATE_COLUMNS, ConfigDef.Type.STRING, "",
                ConfigDef.Importance.HIGH, "写到ClickHouse表中的日期分区字段名, 多个以逗号分隔，不可为空" +
                        " 其值应与ClickHouse对应分布式表一一对应. 即: 需要与clickhouse.sink.tables的参数一一对应. ");
        config.define(CLICKHOUSE_SOURCE_DATE_COLUMNS, ConfigDef.Type.STRING, "",
                ConfigDef.Importance.LOW, "写到ClickHouse表中的时间分区字段所取值的源字段名" +
                        " 1.当此参数为空时, 将默认取当前处理的时间写至clickhouse.sink.date.columns所描述的字段中" +
                        " 2.当此参数不为空时, 其值应与ClickHouse对应分布式表一一对应. 即: 需要与clickhouse.sink.tables的参数一一对应. ");
        config.define(CLICKHOUSE_SOURCE_DATE_FORMAT, ConfigDef.Type.STRING, "yyyy-MM-dd HH:mm:ss",
                ConfigDef.Importance.LOW, "写到ClickHouse表中的时间分区字段所取值的源字段时间的格式" +
                        " 1.当此参数为空时, 将默认yyyy-MM-dd HH:mm:ss格式化" +
                        " 2.当此参数不为空时, 其值应与ClickHouse对应分布式表一一对应. 即: 需要与clickhouse.sink.tables的参数一一对应. ");
    }

    /**
     * 所有参数非空校验
     */
    public static Map<String, ConfigValue> emptyValidate(Map<String, String> connectorConfigs, ConfigDef configDef) {
        Map<String, ConfigValue> configValues = new HashMap<>(16);
        for (ConfigDef.ConfigKey configKey : configDef.configKeys().values()) {
            ConfigValue configValue = new ConfigValue(configKey.name);
            String value = connectorConfigs.get(configKey.name);
            if (configKey.importance != ConfigDef.Importance.LOW) {
                if (StringUtils.isEmpty(value)) {
                    configValue.addErrorMessage(String.format("%s不能为空", configKey.name));
                }
            }
            configValue.value(value);
            configValues.put(configKey.name, configValue);
        }
        return configValues;
    }

    /**
     * 对ClickHouse连接进行校验
     */
    public static JdbcDataSource clickHouseConnectValidate(Map<String, ConfigValue> configValues) {
        ConfigValue hosts = configValues.get(CLICKHOUSE_HOSTS);
        ConfigValue port = configValues.get(CLICKHOUSE_JDBC_PORT);
        ConfigValue user = configValues.get(CLICKHOUSE_JDBC_USER);
        ConfigValue password = configValues.get(CLICKHOUSE_JDBC_PASSWORD);
        ConfigValue sinkDb = configValues.get(CLICKHOUSE_SINK_DATABASE);
        JdbcDataSource dataSource = null;
        if (hosts.errorMessages().isEmpty() && port.errorMessages().isEmpty() && sinkDb.errorMessages().isEmpty()) {
            JdbcConnectConfig connectConfig = JdbcConnectConfig.initCkConnectConfig((String) hosts.value(), (String) port.value(),
                    user == null ? "" : (String) user.value(), password == null ? "" : (String) password.value());
            try {
                dataSource = new JdbcDataSource(connectConfig);
                List<String> databases = dataSource.showDatabases();
                String db = (String) sinkDb.value();
                if (!databases.contains(db)) {
                    sinkDb.addErrorMessage(String.format("%s数据库不存在", (String) sinkDb.value()));
                }
                dataSource.close();
                dataSource = null;
            } catch (Exception e) {
                logger.error("校验ClickHouse连接失败: ", e);
                hosts.addErrorMessage("连接ClickHouse失败, " + e.getMessage());
            }
        }
        return dataSource;
    }

    /**
     * 对分布式表是否存在ClickHouse进行校验
     */
    public static void clusterTableExist(JdbcDataSource dataSource, ConfigValue sinkTablesVal, String sinkDb) {
        if (sinkTablesVal.errorMessages().isEmpty() && sinkTablesVal.value() != null && dataSource != null) {
            String sinkTables = (String) sinkTablesVal.value();
            try {
                List<String> showTables = dataSource.showTables(sinkDb);
                String[] sinkTabArr = sinkTables.split(CONFIG_SEPARATOR);
                for (String tab : sinkTabArr) {
                    if (!showTables.contains(tab)) {
                        sinkTablesVal.addErrorMessage(String.format("%s, 表: %s, 不存在", sinkTablesVal.name(), tab));
                    }
                }
            } catch (SQLException e) {
                sinkTablesVal.addErrorMessage(String.format("%s, 查询ClickHouse失败: %s", sinkTablesVal.name(), e.getMessage()));
                logger.error("查询ClickHouse失败,", e);
            }
        }
    }

    /**
     * 对本地表是否存在ClickHouse进行校验, 本地表会在每个连接下进行校验
     */
    public static void localTableExist(JdbcDataSource dataSource, ConfigValue sinkLocalTablesVal, String sinkDb) {
        if (sinkLocalTablesVal.errorMessages().isEmpty() && sinkLocalTablesVal.value() != null && dataSource != null) {
            String sinkLocalTables = (String) sinkLocalTablesVal.value();
            String[] sinkLocalTabArr = sinkLocalTables.split(CONFIG_SEPARATOR);
            for (String tab : sinkLocalTabArr) {
                try {
                    if (!dataSource.existAllDs(sinkDb, tab)) {
                        sinkLocalTablesVal.addErrorMessage(String.format("%s, 表: %s, 不存在", sinkLocalTablesVal.name(), tab));
                    }
                } catch (SQLException e) {
                    logger.error("查询ClickHouse失败,", e);
                    sinkLocalTablesVal.addErrorMessage(String.format("%s, 查询ClickHouse失败: %s", sinkLocalTablesVal.name(), e.getMessage()));
                }
            }
        }
    }

    /**
     * 校验sink的时间字段个数是否正确；时间字段是否存在对应的表中
     */
    public static void validateSinkDateColumns(Map<String, ConfigValue> configValues, JdbcDataSource dataSource, String sinkDb, ConfigValue sinkTablesVal) {
        ConfigValue sinkDateCols = configValues.get(CLICKHOUSE_SINK_DATE_COLUMNS);
        if (sinkTablesVal.errorMessages().isEmpty() && sinkTablesVal.value() != null && dataSource != null) {
            if (sinkDateCols.errorMessages().isEmpty() && sinkDateCols.value() != null) {
                String[] sinkTabArr = ((String) sinkTablesVal.value()).split(CONFIG_SEPARATOR);
                String[] sinkDateColArr = ((String) sinkDateCols.value()).split(CONFIG_SEPARATOR);
                if (sinkDateColArr.length != sinkTabArr.length) {
                    sinkDateCols.addErrorMessage(sinkTablesVal.name() + "需要与sink table个数一一对应");
                    return;
                }
                for (int i = 0; i < sinkTabArr.length; i++) {
                    try {
                        List<String> columns = dataSource.descTable(String.format("`%s`.`%s`", sinkDb, sinkTabArr[i]));
                        if (!columns.contains(sinkDateColArr[i])) {
                            sinkDateCols.addErrorMessage(String.format("%s, 字段: %s, 不存在table: %s中", sinkTablesVal.name(), sinkDateColArr[i], sinkTabArr[i]));
                        }
                    } catch (SQLException e) {
                        logger.error("查询ClickHouse失败,", e);
                        sinkDateCols.addErrorMessage(String.format("%s, 查询ClickHouse失败: %s", sinkDateCols.name(), e.getMessage()));
                    }
                }
            }
        }
    }

    /**
     * 校验数据源中的date字段如果不为空其个数是否与sink table 一致
     */
    public static void validateSourceDateColumns(Map<String, ConfigValue> configValues, JdbcDataSource dataSource, ConfigValue sinkTablesVal) {
        ConfigValue sourceDateCols = configValues.get(CLICKHOUSE_SOURCE_DATE_COLUMNS);
        if (sinkTablesVal.errorMessages().isEmpty() && sinkTablesVal.value() != null && dataSource != null) {
            if (sourceDateCols.errorMessages().isEmpty() && sourceDateCols.value() != null) {
                String[] sinkTabArr = ((String) sinkTablesVal.value()).split(CONFIG_SEPARATOR);
                String[] sourceDateColArr = ((String) sourceDateCols.value()).split(CONFIG_SEPARATOR);
                if (sourceDateColArr.length != sinkTabArr.length) {
                    sourceDateCols.addErrorMessage(sinkTablesVal.name() + "需要与sink table个数一一对应");
                }
            }
        }
    }

    /**
     * 校验输入的日期格式如果不为空其个数是否与sink table 一致且是否是正确的日期格式化格式
     */
    public static void validateDateFormat(Map<String, ConfigValue> configValues, JdbcDataSource dataSource, ConfigValue sinkTablesVal) {
        ConfigValue dateFormat = configValues.get(CLICKHOUSE_SOURCE_DATE_FORMAT);
        if (sinkTablesVal.errorMessages().isEmpty() && sinkTablesVal.value() != null && dataSource != null) {
            if (dateFormat.errorMessages().isEmpty() && dateFormat.value() != null) {
                String[] sinkTabArr = ((String) sinkTablesVal.value()).split(CONFIG_SEPARATOR);
                String[] dateFormatArr = ((String) dateFormat.value()).split(CONFIG_SEPARATOR);
                if (dateFormatArr.length != sinkTabArr.length) {
                    dateFormat.addErrorMessage(dateFormat.name() + "需要与sink table个数一一对应");
                    return;
                }
                for (String df : dateFormatArr) {
                    try {
                        SimpleDateFormat sdf = new SimpleDateFormat(df);
                        sdf.format(new Date());
                    } catch (Exception e) {
                        logger.error("校验输入时间格式: ", e);
                        dateFormat.addErrorMessage(String.format("%s, 值: %s, 不是正确的日期格式化格式", dateFormat.name(), df));
                    }
                }
            }
        }
    }

}
