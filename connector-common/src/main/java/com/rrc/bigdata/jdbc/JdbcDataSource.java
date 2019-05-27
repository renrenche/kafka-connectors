package com.rrc.bigdata.jdbc;

import org.apache.commons.lang3.RandomUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @author L
 */
public class JdbcDataSource {

    private static final Logger logger = LoggerFactory.getLogger(JdbcDataSource.class);

    private final Map<String, Connection> dataSourceMap = new ConcurrentHashMap<>();

    public JdbcDataSource(JdbcConnectConfig config) throws SQLException {
        for (String jdbcUrl : config.getJdbcUrl()) {
            DriverManager.setLoginTimeout(10);
            Properties properties = new Properties();
            properties.setProperty("user", config.getUser());
            properties.setProperty("password", config.getPassword());
            properties.setProperty("keepAliveTimeout", "10000");
            Connection connection = DriverManager.getConnection(jdbcUrl, properties);
            dataSourceMap.put(jdbcUrl, connection);
            logger.info("jdbc连接, jdbcUrl: " + jdbcUrl);
        }
    }

    public void executeAllDs(String sql) throws SQLException {
        for (Map.Entry<String, Connection> entry : dataSourceMap.entrySet()) {
            try (PreparedStatement statement = entry.getValue().prepareStatement(sql)) {
                statement.execute();
            } catch (SQLException e) {
                logger.error("执行sql出现错误: " + sql);
                throw e;
            }
        }
    }

    public void execute(String sql) throws SQLException {
        Connection connection = randomDataSource();
        try (PreparedStatement statement = connection.prepareStatement(sql)) {
            statement.execute();
        } catch (SQLException e) {
            logger.error("执行sql出现错误: " + sql);
            throw e;
        }
    }

    public List<String> showDatabases() throws SQLException {
        Connection connection = randomDataSource();
        String sql = "show databases";
        return firstColumnResult(connection, sql);
    }

    public List<String> showTables(String database) throws SQLException {
        Connection connection = randomDataSource();
        String sql = String.format("show tables from `%s`", database);
        return firstColumnResult(connection, sql);
    }

    public List<String> descTable(String table) throws SQLException {
        Connection connection = randomDataSource();
        String sql = String.format("desc %s", table);
        return firstColumnResult(connection, sql);
    }

    public Map<String, String> descTableColType(String table) throws SQLException {
        Connection connection = randomDataSource();
        String sql = String.format("desc %s", table);
        try (PreparedStatement statement = connection.prepareStatement(sql)) {
            ResultSet resultSet = statement.executeQuery();
            Map<String, String> colTypes = new HashMap<>(16);
            while (resultSet.next()) {
                colTypes.put(resultSet.getString(1), resultSet.getString(2));
            }
            return colTypes;
        } catch (SQLException e) {
            logger.error("执行sql出现错误: " + sql);
            throw e;
        }
    }

    public boolean existAllDs(String database, String localTable) throws SQLException {
        String sql = String.format("show tables from `%s`", database);
        for (Map.Entry<String, Connection> entry : dataSourceMap.entrySet()) {
            Connection connection = entry.getValue();
            try (PreparedStatement statement = connection.prepareStatement(sql)) {
                ResultSet resultSet = statement.executeQuery();
                List<String> dbs = new ArrayList<>();
                while (resultSet.next()) {
                    dbs.add(resultSet.getString(1));
                }
                if (!dbs.contains(localTable)) {
                    logger.warn(String.format("jdbc 库: %s, 表: %s,不存在, 连接url: %s", database, localTable, entry.getKey()));
                    return true;
                }
            } catch (SQLException e) {
                logger.error("执行sql出现错误: " + sql);
                throw e;
            }
        }
        return true;
    }

    public void close() {
        logger.info("关闭jdbc的连接");
        for (Map.Entry<String, Connection> entry : dataSourceMap.entrySet()) {
            Connection dataSource = entry.getValue();
            try {
                dataSource.close();
            } catch (SQLException e) {
                logger.warn("关闭jdbc连接出错, ", e);
            }
        }
    }

    private List<String> firstColumnResult(Connection connection, String sql) throws SQLException {
        try (PreparedStatement statement = connection.prepareStatement(sql)) {
            ResultSet resultSet = statement.executeQuery();
            List<String> dbs = new ArrayList<>();
            while (resultSet.next()) {
                dbs.add(resultSet.getString(1));
            }
            return dbs;
        } catch (SQLException e) {
            logger.error("执行sql出现错误: " + sql);
            throw e;
        }
    }

    private Connection randomDataSource() {
        if (dataSourceMap.size() == 1) {
            return dataSourceMap.values().iterator().next();
        }
        Set<Map.Entry<String, Connection>> entrySet = dataSourceMap.entrySet();
        List<Map.Entry<String, Connection>> entryArrayList = new ArrayList<>(entrySet);
        int randomIdx = RandomUtils.nextInt(0, entryArrayList.size());
        return entryArrayList.get(randomIdx).getValue();
    }

}