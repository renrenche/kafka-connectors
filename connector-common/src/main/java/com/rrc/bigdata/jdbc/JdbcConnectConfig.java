package com.rrc.bigdata.jdbc;

import org.apache.commons.lang3.StringUtils;

import java.util.HashSet;
import java.util.Set;

import static com.rrc.bigdata.constant.CommonConstant.CLICKHOUSE_DRIVER;

/**
 * @author L
 */
public class JdbcConnectConfig {

    private Set<String> jdbcUrl = new HashSet<>();

    private String user;

    private String password;

    private String driverClass;

    public static JdbcConnectConfig initCkConnectConfig(String hosts, String port, String user, String password) {
        JdbcConnectConfig connectConfig = new JdbcConnectConfig();
        String[] hostArr = hosts.split(",");
        for (String ho : hostArr) {
            connectConfig.addJdbcUrl(String.format("jdbc:clickhouse://%s:%s", ho, port));
        }
        connectConfig.setDriverClass(CLICKHOUSE_DRIVER);
        connectConfig.setUser(StringUtils.isEmpty(user) ? "" : user);
        connectConfig.setPassword(StringUtils.isEmpty(password) ? "" : password);
        return connectConfig;
    }

    public void addJdbcUrl(String jdbcUrl) {
        this.jdbcUrl.add(jdbcUrl);
    }

    public Set<String> getJdbcUrl() {
        return jdbcUrl;
    }

    public void setJdbcUrl(Set<String> jdbcUrl) {
        this.jdbcUrl = jdbcUrl;
    }

    public String getUser() {
        return user;
    }

    public void setUser(String user) {
        this.user = user;
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    public String getDriverClass() {
        return driverClass;
    }

    public void setDriverClass(String driverClass) {
        this.driverClass = driverClass;
    }
}
