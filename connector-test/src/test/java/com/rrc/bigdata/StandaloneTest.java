package com.rrc.bigdata;

import org.apache.http.HttpEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import org.apache.kafka.connect.cli.ConnectStandalone;
import org.junit.Test;


public class StandaloneTest {

    @Test
    public void runTest() throws Exception {
        String[] params = new String[2];
        String standalone = this.getClass().getClassLoader().getResource("connect-standalone.properties").getPath();
        System.out.println(standalone);
        params[0] = standalone;
        String sink = this.getClass().getClassLoader().getResource("json-sink-clickhouse.properties").getPath();
        System.out.println(sink);
        params[1] = sink;
        ConnectStandalone.main(params);

    }

    @Test
    public void validateTest() throws Exception {
        String host = "localhost:8083";
        String connectorClass = "com.rrc.bigdata.connector.JsonSinkClickHouseConnector";
        String params = String.format("{" +
                "        \"name\": \"SinkClickHouseConnector\"," +
                "        \"connector.class\": \"%s\"," +
                "        \"topics\": \"topic\"," +
                "        \"tasks.max\": \"1\"," +
                "        \"clickhouse.jdbc.port\": \"\"," +
                "        \"clickhouse.optimize\": \"\"," +
                "        \"clickhouse.sink.tables\": \"\"," +
                "        \"clickhouse.hosts\": \"s\"," +
                "        \"clickhouse.sink.database\": \"\"" +
                "    }", connectorClass);

        String url = String.format("http://%s/connector-plugins/%s/config/validate", host, connectorClass);
        CloseableHttpClient httpClient = HttpClients.createDefault();
        HttpPut put = new HttpPut(url);
        StringEntity stringEntity = new StringEntity(params);
        put.setEntity(stringEntity);
        put.setHeader("Content-Type", "application/json;charset=UTF-8");
        CloseableHttpResponse httpResponse = httpClient.execute(put);
        HttpEntity entity = httpResponse.getEntity();
        String result = EntityUtils.toString(entity, "UTF-8");

        System.out.println(result);
    }


}
