package com.rrc.bigdata.connector;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ContainerNode;
import com.rrc.bigdata.clickhouse.ClickHouseHelper;
import com.rrc.bigdata.clickhouse.ClickHouseTypeConvert;
import com.rrc.bigdata.jdbc.JdbcConnectConfig;
import com.rrc.bigdata.jdbc.JdbcDataSource;
import com.rrc.bigdata.model.ClickHouseSinkData;
import com.rrc.bigdata.model.ClickHouseTableInfo;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.SQLException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

import static com.rrc.bigdata.constant.CommonConstant.*;
import static com.rrc.bigdata.constant.CommonConstant.CLICKHOUSE_SINK_DATABASE;

/**
 * @author L
 */
public class JsonSinkClickHouseTask extends SinkTask {
    private static final Logger logger = LoggerFactory.getLogger(JsonSinkClickHouseTask.class);

    private ClickHouseHelper clickHouseHelper = new ClickHouseHelper();
    private JdbcDataSource dataSource;
    private String sinkDb;
    private boolean optimize = false;
    private Map<String, ClickHouseTableInfo> sinkTableMap = new ConcurrentHashMap<>(16);
    private SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");

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
        String inOptimize = props.get(CLICKHOUSE_OPTIMIZE);
        if (inOptimize != null && BOOLEAN_TRUE.equals(inOptimize.toLowerCase())) {
            optimize = true;
        }
        parseSinkTables(props);
        logger.info("start task, 开始连接ClickHouse");
    }

    @Override
    public void put(Collection<SinkRecord> records) {
        Map<String, ClickHouseSinkData> insertMap = new HashMap<>(16);
        ObjectMapper mapper = new ObjectMapper();
        Map<String, Map<String, String>> tableColumns = new HashMap<>();
        try {
            for (SinkRecord record : records) {
                logger.debug("消费的offset: " + record.kafkaOffset());
                try {
                    ClickHouseSinkData ckData = handlerRecord(mapper, tableColumns, record);
                    ckData.setOptimize(optimize);
                    clickHouseHelper.collectInsertData(insertMap, ckData);
                } catch (IOException e) {
                    logger.error("消费到的数据不能被json解析, 数据: " + record.value(), e);
                }
            }
            clickHouseHelper.batchInsert(dataSource, sinkDb, insertMap);
        } catch (SQLException e) {
            logger.error("执行ClickHouse SQL失败,", e);
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
    private ClickHouseSinkData handlerRecord(ObjectMapper mapper, Map<String, Map<String, String>> tableColumns, SinkRecord record) throws IOException, SQLException, ParseException {
        JsonNode jsonNode = mapper.readTree((String) record.value());
        String topic = record.topic();
        ClickHouseTableInfo clickHouseTableInfo = sinkTableMap.get(topic);
        Map<String, String> ckTabColumns = tableColumns.get(clickHouseTableInfo.getTable());
        if (ckTabColumns == null) {
            ckTabColumns = dataSource.descTableColType(String.format("`%s`.`%s`", sinkDb, clickHouseTableInfo.getTable()));
            tableColumns.put(clickHouseTableInfo.getTable(), ckTabColumns);
        }
        ClickHouseSinkData clickHouseSinkData = new ClickHouseSinkData();
        List<String> columns = new ArrayList<>();
        List<Object> values = new ArrayList<>();
        Iterator<String> keys = jsonNode.fieldNames();
        while (keys.hasNext()) {
            String fieldName = keys.next();
            String colType = ckTabColumns.get(fieldName);
            if (colType != null) {
                columns.add("`" + fieldName + "`");
                JsonNode nodeValue = jsonNode.get(fieldName);
                if (ClickHouseTypeConvert.isNumberType(colType)) {
                    values.add(nodeValue.asText());
                } else if (nodeValue instanceof ContainerNode) {
                    values.add(String.format("'%s'", nodeValue.toString().replaceAll("'", "\\\\'")));
                } else {
                    values.add(String.format("'%s'", nodeValue.asText().replaceAll("'", "\\\\'")));
                }
            } else {
                throw new ConfigException(String.format("topic: %s, 列: %s, 不存在ClickHouse表: %s中, 该表所有列: %s",
                        topic, fieldName, clickHouseTableInfo.getTable(), StringUtils.join(ckTabColumns.keySet(), ", ")));
            }
        }
        columns.add("`" + clickHouseTableInfo.getSinkDateCol() + "`");
        if (StringUtils.isNotEmpty(clickHouseTableInfo.getSourceDateCol())) {
            JsonNode sourceDateNode = jsonNode.get(clickHouseTableInfo.getSourceDateCol());
            if (sourceDateNode != null) {
                values.add(String.format("'%s'", dateFormat.format(clickHouseTableInfo.getDf().parse(sourceDateNode.asText()))));
            } else {
                values.add(String.format("'%s'", dateFormat.format(System.currentTimeMillis())));
            }
        } else {
            values.add(String.format("'%s'", dateFormat.format(System.currentTimeMillis())));
        }
        clickHouseSinkData.setTable(clickHouseTableInfo.getTable());
        clickHouseSinkData.setLocalTable(clickHouseTableInfo.getLocalTable());
        clickHouseSinkData.setColumns(StringUtils.join(columns, ", "));
        clickHouseSinkData.putValue(String.format("(%s)", StringUtils.join(values, ", ")));
        return clickHouseSinkData;
    }

    /**
     * 解析输入的 ClickHouse 相关的参数配置, 分布式表, 本地表, 表分区字段, 日期数据源字段, 日期数据源字段的格式
     * 将解析完的配置存入 sinkTableMap 中, key 为topic, value 为该 topic 的写入的 ClickHouse 表信息
     */
    private void parseSinkTables(Map<String, String> props) {
        String sinkLocalTables = props.get(CLICKHOUSE_SINK_LOCAL_TABLES);
        String sourceDateCols = props.get(CLICKHOUSE_SOURCE_DATE_COLUMNS);
        String sourceDateFormat = props.get(CLICKHOUSE_SOURCE_DATE_FORMAT);
        String[] topicArr = props.get(TOPICS).split(CONFIG_SEPARATOR);
        String[] sinkTabArr = props.get(CLICKHOUSE_SINK_TABLES).split(CONFIG_SEPARATOR);
        String[] sinkDateCols = props.get(CLICKHOUSE_SINK_DATE_COLUMNS).split(CONFIG_SEPARATOR);

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
            clickHouseTableInfo.setTable(sinkTabArr[i]);
            clickHouseTableInfo.setSinkDateCol(sinkDateCols[i]);
            if (StringUtils.isNotEmpty(sinkLocalTables)) {
                clickHouseTableInfo.setLocalTable(sinkLocalTabArr[i]);
            } else {
                clickHouseTableInfo.setLocalTable(sinkTabArr[i] + DEFAULT_LOCAL_TABLE_SUFFIX);
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

}
