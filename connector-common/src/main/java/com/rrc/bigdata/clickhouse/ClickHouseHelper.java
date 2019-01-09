package com.rrc.bigdata.clickhouse;

import com.rrc.bigdata.jdbc.JdbcDataSource;
import com.rrc.bigdata.model.ClickHouseSinkData;
import org.apache.commons.lang3.StringUtils;

import java.sql.SQLException;
import java.util.Map;

/**
 * @author L
 */
public class ClickHouseHelper {

    /**
     * ClickHouse sink 辅助方法, 将相同表, 相同字段列表的数据, 存至同一个 ClickHouseSinkData 对象
     *
     * @param insertMap key 规则为表名_字段列表, value 为 ClickHouseSinkData 对象
     * @param sinkData    待整合至 insertMap 的数据
     */
    public void collectInsertData(Map<String, ClickHouseSinkData> insertMap, ClickHouseSinkData sinkData) {
        if (sinkData != null) {
            String key = String.format("%s_%s", sinkData.getTable(), sinkData.getColumns());
            ClickHouseSinkData mapCkData = insertMap.get(key);
            if (mapCkData != null) {
                mapCkData.putValue(sinkData.getValues());
                if (sinkData.isOptimize()) {
                    mapCkData.setOptimize(sinkData.isOptimize());
                }
            } else {
                insertMap.put(key, sinkData);
            }
        }
    }

    /**
     * 将 insertMap 中的数据批量写入 ClickHouse 中
     *
     * @param dataSource ClickHouse jdbc
     * @param database     数据库
     * @param insertMap  key 规则为表名_字段列表, value 为 ClickHouseSinkData 对象
     * @throws SQLException jdbc 错误
     */
    public void batchInsert(JdbcDataSource dataSource, String database, Map<String, ClickHouseSinkData> insertMap) throws SQLException {
        for (Map.Entry<String, ClickHouseSinkData> entry : insertMap.entrySet()) {
            String insert = String.format("insert into `%s`.`%s` (%s) values %s", database, entry.getValue().getTable(),
                    entry.getValue().getColumns(), StringUtils.join(entry.getValue().getValues(), ", "));
            dataSource.execute(insert);
            if (entry.getValue().isOptimize()) {
                String optimize = String.format("optimize table `%s`.`%s`", database, entry.getValue().getLocalTable());
                dataSource.executeAllDs(optimize);
            }
        }
    }
}
