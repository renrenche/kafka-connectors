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
        try {
            for (Map.Entry<String, Connection> entry : dataSourceMap.entrySet()) {
                PreparedStatement statement = entry.getValue().prepareStatement(sql);
                statement.execute();
                statement.close();
            }
        } catch (SQLException e) {
            logger.error("执行sql出现错误: " + sql);
            throw e;
        }
    }

    public void execute(String sql) throws SQLException {
        try {
            Connection connection = randomDataSource();
            PreparedStatement statement = connection.prepareStatement(sql);
            statement.execute();
            statement.close();
        } catch (SQLException e) {
            logger.error("执行sql出现错误: " + sql);
            throw e;
        }
    }

    public List<String> showDatabases() throws SQLException {
        Connection connection = randomDataSource();
        PreparedStatement statement = connection.prepareStatement("show databases");
        ResultSet resultSet = statement.executeQuery();
        List<String> dbs = new ArrayList<>();
        while (resultSet.next()) {
            dbs.add(resultSet.getString(1));
        }
        statement.close();
        return dbs;
    }

    public List<String> showTables(String database) throws SQLException {
        Connection connection = randomDataSource();
        PreparedStatement statement = connection.prepareStatement(String.format("show tables from `%s`", database));
        ResultSet resultSet = statement.executeQuery();
        List<String> dbs = new ArrayList<>();
        while (resultSet.next()) {
            dbs.add(resultSet.getString(1));
        }
        statement.close();
        return dbs;
    }

    public List<String> descTable(String table) throws SQLException {
        Connection connection = randomDataSource();
        PreparedStatement statement = connection.prepareStatement(String.format("desc %s", table));
        ResultSet resultSet = statement.executeQuery();
        List<String> dbs = new ArrayList<>();
        while (resultSet.next()) {
            dbs.add(resultSet.getString(1));
        }
        statement.close();
        return dbs;
    }

    public Map<String, String> descTableColType(String table) throws SQLException {
        Connection connection = randomDataSource();
        PreparedStatement statement = connection.prepareStatement(String.format("desc %s", table));
        ResultSet resultSet = statement.executeQuery();
        Map<String, String> colTypes = new HashMap<>(16);
        while (resultSet.next()) {
            colTypes.put(resultSet.getString(1), resultSet.getString(2));
        }
        statement.close();
        return colTypes;
    }

    public boolean existAllDs(String database, String localTable) throws SQLException {
        for (Map.Entry<String, Connection> entry : dataSourceMap.entrySet()) {
            Connection connection = entry.getValue();
            PreparedStatement statement = connection.prepareStatement(String.format("show tables from `%s`", database));
            ResultSet resultSet = statement.executeQuery();
            List<String> dbs = new ArrayList<>();
            while (resultSet.next()) {
                dbs.add(resultSet.getString(1));
            }
            if (!dbs.contains(localTable)) {
                logger.warn(String.format("jdbc 库: %s, 表: %s,不存在, 连接url: %s", database, localTable, entry.getKey()));
                return true;
            }
            statement.close();
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
