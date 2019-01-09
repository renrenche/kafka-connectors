package com.rrc.bigdata.connector;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.rrc.bigdata.clickhouse.ClickHouseHelper;
import com.rrc.bigdata.constant.Constant;
import com.rrc.bigdata.jdbc.JdbcConnectConfig;
import com.rrc.bigdata.jdbc.JdbcDataSource;
import com.rrc.bigdata.model.BinlogDateConvert;
import com.rrc.bigdata.model.ClickHouseSinkData;
import com.rrc.bigdata.model.ClickHouseTableInfo;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.connect.data.*;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

import static com.rrc.bigdata.constant.CommonConstant.*;

/**
 * WorkerSinkTask{id=mysql-sink-hdfs-0} Committing offsets (org.apache.kafka.connect.runtime.WorkerSinkTask:261)
 *
 * @author L
 */
public class MySqlSinkClickHouseTask extends SinkTask {
    private static final Logger logger = LoggerFactory.getLogger(MySqlSinkClickHouseTask.class);

    private ClickHouseHelper clickHouseHelper = new ClickHouseHelper();
    private JdbcDataSource dataSource;
    private String sinkDb;
    private Map<String, ClickHouseTableInfo> sinkTableMap = new ConcurrentHashMap<>(16);
    private SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
    private boolean optimize = true;

    @Override
    public String version() {
        return "1.0";
    }

    @Override
    public void start(Map<String, String> props) {
        String hosts = props.get(CLICKHOUSE_HOSTS);
        String port = props.get(CLICKHOUSE_JDBC_PORT);
        String user = props.get(CLICKHOUSE_JDBC_USER);
        String password = props.get(CLICKHOUSE_JDBC_PASSWORD);
        sinkDb = props.get(CLICKHOUSE_SINK_DATABASE);
        JdbcConnectConfig connectConfig = JdbcConnectConfig.initCkConnectConfig(hosts, port, user, password);
        dataSource = new JdbcDataSource(connectConfig);
        logger.info("start task, 开始连接ClickHouse");
        String inOptimize = props.get(CLICKHOUSE_OPTIMIZE);
        if (inOptimize != null && BOOLEAN_FALSE.equals(inOptimize.toLowerCase())) {
            optimize = false;
        }
        parseSinkTables(props);
    }

    @Override
    public void put(Collection<SinkRecord> records) {
        Map<String, ClickHouseSinkData> insertMap = new HashMap<>(16);
        try {
            for (SinkRecord record : records) {
                logger.debug("消费的offset: " + record.kafkaOffset());
                ClickHouseSinkData clickHouseSinkData = handlerSinkRecord(record);
                if (clickHouseSinkData != null && !optimize) {
                    clickHouseSinkData.setOptimize(false);
                }
                clickHouseHelper.collectInsertData(insertMap, clickHouseSinkData);
            }
            clickHouseHelper.batchInsert(dataSource, sinkDb, insertMap);
        } catch (SQLException e) {
            logger.error("执行ClickHouse SQL失败, ", e);
            throw new ConfigException(e.getMessage());
        } catch (ParseException e) {
            logger.error("日期格式解析错误,", e);
            throw new ConfigException(e.getMessage());
        }
    }

    @Override
    public void flush(Map<TopicPartition, OffsetAndMetadata> offsets) {
    }

    @Override
    public void stop() {
        if (dataSource != null) {
            try {
                dataSource.close();
                logger.info("关闭ClickHouse连接成功");
            } catch (SQLException e) {
                logger.error("关闭ClickHouse连接失败, ", e);
            }
        }
    }

    /**
     * 解析每条 json 数据, 并将表信息和数据信息封装为 ClickHouseSinkData 对象
     */
    private ClickHouseSinkData handlerSinkRecord(SinkRecord record) throws SQLException, ParseException {
        logger.debug("record 的值: " + record.value());
        Map<String, Object> jsonMapValue = toJsonMap("record.value", (Struct) record.value());
        String op = (String) jsonMapValue.get(Constant.VALUE_KEY_OP);
        if (Constant.OP_INSERT.equals(op) || Constant.OP_UPDATE.equals(op)) {
            ClickHouseSinkData clickHouseSinkData = new ClickHouseSinkData();
            Map<String, Object> sourceMap = (Map<String, Object>) jsonMapValue.get(Constant.VALUE_KEY_SOURCE);
            String table = (String) sourceMap.get(Constant.VALUE_KEY_SOURCE_TABLE);
            initInsertCkData(record, op, clickHouseSinkData, table);
            List<String> ckTableCol = dataSource.descTable(String.format("`%s`.`%s`", sinkDb, clickHouseSinkData.getTable()));
            Map<String, Object> afterMap = (Map<String, Object>) jsonMapValue.get(Constant.VALUE_KEY_AFTER);
            List<String> columns = new ArrayList<>();
            List<Object> values = new ArrayList<>();
            for (Map.Entry<String, Object> entry : afterMap.entrySet()) {
                if (ckTableCol.contains(entry.getKey())) {
                    columns.add("`" + entry.getKey() + "`");
                    if (entry.getValue() instanceof String) {
                        values.add(String.format("'%s'", entry.getValue().toString()
                                .replaceAll("\\\\", "\\\\\\\\")
                                .replaceAll("'", "\\\\'")));
                    } else {
                        values.add(entry.getValue());
                    }
                } else {
                    throw new ConfigException(String.format("binlog表: %s, 列: %s, 不存在ClickHouse表: %s中, 该表所有列: %s",
                            table, entry.getKey(), clickHouseSinkData.getTable(), StringUtils.join(ckTableCol, ", ")));
                }
            }
            ClickHouseTableInfo clickHouseTableInfo = sinkTableMap.get(record.topic());
            columns.add("`" + clickHouseTableInfo.getSinkDateCol() + "`");
            if (StringUtils.isNotEmpty(clickHouseTableInfo.getSourceDateCol())) {
                Object sourceDataVal = afterMap.get(clickHouseTableInfo.getSourceDateCol());
                if (sourceDataVal != null) {
                    values.add(String.format("'%s'", dateFormat.format(clickHouseTableInfo.getDf().parse((String) sourceDataVal))));
                } else {
                    values.add(String.format("'%s'", dateFormat.format(System.currentTimeMillis())));
                }
            } else {
                values.add(String.format("'%s'", dateFormat.format(System.currentTimeMillis())));
            }
            clickHouseSinkData.setColumns(StringUtils.join(columns, ", "));
            clickHouseSinkData.putValue(String.format("(%s)", StringUtils.join(values, ", ")));
            return clickHouseSinkData;
        } else {
            logger.error(String.format("op值: %s, 非insert和update操作", op));
            for (Map.Entry<String, Object> entry : jsonMapValue.entrySet()) {
                logger.error(String.format("参数key: %s, 参数value: %s", entry.getKey(), entry.getValue()));
            }
        }
        return null;
    }

    private void initInsertCkData(SinkRecord record, String op, ClickHouseSinkData clickHouseSinkData, String table) {
        ClickHouseTableInfo clickHouseTableInfo = sinkTableMap.get(record.topic());
        if (clickHouseTableInfo == null) {
            clickHouseSinkData.setTable(table);
            clickHouseSinkData.setLocalTable(table + DEFAULT_LOCAL_TABLE_SUFFIX);
        } else {
            if (StringUtils.isEmpty(clickHouseTableInfo.getTable())) {
                clickHouseSinkData.setTable(table);
            } else {
                clickHouseSinkData.setTable(clickHouseTableInfo.getTable());
            }
            if (StringUtils.isEmpty(clickHouseTableInfo.getLocalTable())) {
                clickHouseSinkData.setLocalTable(clickHouseSinkData.getTable() + DEFAULT_LOCAL_TABLE_SUFFIX);
            } else {
                clickHouseSinkData.setLocalTable(clickHouseTableInfo.getLocalTable());
            }
        }
        if (Constant.OP_UPDATE.equals(op)) {
            clickHouseSinkData.setOptimize(true);
        }
    }

    /**
     * 解析输入的 ClickHouse 相关的参数配置, 分布式表, 本地表, 表分区字段, 日期数据源字段, 日期数据源字段的格式
     * 将解析完的配置存入 sinkTableMap 中, key 为topic, value 为该 topic 的写入的 ClickHouse 表信息
     */
    private void parseSinkTables(Map<String, String> props) {
        String sinkLocalTables = props.get(CLICKHOUSE_SINK_LOCAL_TABLES);
        String sourceDateCols = props.get(CLICKHOUSE_SOURCE_DATE_COLUMNS);
        String sourceDateFormat = props.get(CLICKHOUSE_SOURCE_DATE_FORMAT);
        String sinkTables = props.get(CLICKHOUSE_SINK_TABLES);
        String[] topicArr = props.get(TOPICS).split(CONFIG_SEPARATOR);
        String[] sinkDateCols = props.get(CLICKHOUSE_SINK_DATE_COLUMNS).split(CONFIG_SEPARATOR);

        String[] sinkTabArr = new String[]{};
        if (StringUtils.isNotEmpty(sinkTables)) {
            sinkTabArr = sinkTables.split(CONFIG_SEPARATOR);
        }
        String[] sinkLocalTabArr = new String[]{};
        if (StringUtils.isNotEmpty(sinkLocalTables)) {
            sinkLocalTabArr = sinkLocalTables.split(CONFIG_SEPARATOR);
        }
        String[] sourceDateColArr = new String[]{};
        if (StringUtils.isNotEmpty(sourceDateCols)) {
            sourceDateColArr = sourceDateCols.split(CONFIG_SEPARATOR);
        }
        String[] sourceDfArr = new String[]{};
        if (StringUtils.isNotEmpty(sourceDateFormat)) {
            sourceDfArr = sourceDateFormat.split(CONFIG_SEPARATOR);
        }

        for (int i = 0; i < topicArr.length; i++) {
            ClickHouseTableInfo clickHouseTableInfo = new ClickHouseTableInfo();
            if (StringUtils.isNotEmpty(sinkTables)) {
                clickHouseTableInfo.setTable(sinkTabArr[i]);
            }
            clickHouseTableInfo.setSinkDateCol(sinkDateCols[i]);
            if (StringUtils.isNotEmpty(sinkLocalTables)) {
                clickHouseTableInfo.setLocalTable(sinkLocalTabArr[i]);
            }
            if (StringUtils.isNotEmpty(sourceDateCols)) {
                clickHouseTableInfo.setSourceDateCol(sourceDateColArr[i]);
            }
            if (StringUtils.isNotEmpty(sourceDateFormat)) {
                clickHouseTableInfo.setDf(new SimpleDateFormat(sourceDfArr[i]));
            } else {
                clickHouseTableInfo.setDf(new SimpleDateFormat(DEFAULT_DATE_FORMAT));
            }
            sinkTableMap.put(topicArr[i], clickHouseTableInfo);
        }
    }

    /**
     * 解析消费到 kafka 的 MySQL 的数据, 也就是 Struct 对象
     */
    private Map<String, Object> toJsonMap(String inField, Struct struct) {
        ObjectMapper mapper = new ObjectMapper();
        Map<String, Object> jsonMap = new HashMap<>(16);
        if (struct == null) {
            logger.warn(String.format("fieldName: %s, struct 为空", inField));
            return jsonMap;
        }
        List<Field> fields = struct.schema().fields();
        for (Field field : fields) {
            String fieldName = field.name();
            String schemaName = field.schema().name();
            switch (field.schema().type()) {
                case INT8:
                    jsonMap.put(fieldName, struct.getInt8(fieldName));
                    break;
                case INT16:
                    jsonMap.put(fieldName, struct.getInt16(fieldName));
                    break;
                case INT32:
                    if (StringUtils.isNotEmpty(schemaName)) {
                        if (struct.getInt32(fieldName) == null) {
                            jsonMap.put(fieldName, 0);
                        } else {
                            jsonMap.put(fieldName, BinlogDateConvert.dateConvert(schemaName, Long.valueOf(struct.getInt32(fieldName))));
                        }
                    } else {
                        jsonMap.put(fieldName, struct.getInt32(fieldName));
                    }
                    break;
                case INT64:
                    if (StringUtils.isNotEmpty(schemaName)) {
                        if (struct.getInt64(fieldName) == null) {
                            jsonMap.put(fieldName, 0);
                        } else {
                            jsonMap.put(fieldName, BinlogDateConvert.dateConvert(schemaName, struct.getInt64(fieldName)));
                        }
                    } else {
                        jsonMap.put(fieldName, struct.getInt64(fieldName));
                    }
                    break;
                case FLOAT32:
                    jsonMap.put(fieldName, struct.getFloat32(fieldName));
                    break;
                case FLOAT64:
                    jsonMap.put(fieldName, struct.getFloat64(fieldName));
                    break;
                case BOOLEAN:
                    jsonMap.put(fieldName, struct.getBoolean(fieldName));
                    break;
                case STRING:
                    if (struct.getString(fieldName) == null) {
                        jsonMap.put(fieldName, "");
                    } else if (StringUtils.isNotEmpty(schemaName)) {
                        jsonMap.put(fieldName, BinlogDateConvert.dateConvert(schemaName, struct.getString(fieldName)));
                    } else {
                        jsonMap.put(fieldName, struct.getString(fieldName));
                    }
                    break;
                case BYTES:
                    if (struct.getBytes(fieldName) == null) {
                        jsonMap.put(fieldName, "");
                    } else {
                        jsonMap.put(fieldName, Arrays.toString(struct.getBytes(fieldName)));
                    }
                    break;
                case ARRAY:
                    if (struct.getArray(fieldName) == null) {
                        jsonMap.put(fieldName, "");
                    } else {
                        try {
                            jsonMap.put(fieldName, mapper.writeValueAsString(struct.getArray(fieldName)));
                        } catch (JsonProcessingException e) {
                            logger.error("json to string", e);
                        }
                    }
                    break;
                case MAP:
                    if (struct.getMap(fieldName) == null) {
                        jsonMap.put(fieldName, "");
                    } else {
                        try {
                            jsonMap.put(fieldName, mapper.writeValueAsString(struct.getMap(fieldName)));
                        } catch (JsonProcessingException e) {
                            logger.error("json to string", e);
                        }
                    }
                    break;
                case STRUCT:
                    jsonMap.put(fieldName, toJsonMap(fieldName, struct.getStruct(fieldName)));
                    break;
                default:
                    break;
            }
        }
        return jsonMap;
    }

}
