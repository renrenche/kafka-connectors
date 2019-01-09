import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ContainerNode;
import com.rrc.bigdata.jdbc.JdbcConnectConfig;
import com.rrc.bigdata.jdbc.JdbcDataSource;
import org.junit.Test;

import java.io.IOException;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Iterator;
import java.util.Map;

public class JacksonTest {

    @Test
    public void readJson() throws IOException {
        ObjectMapper mapper = new ObjectMapper();
//        String json = "{\"name\":\"tom\",\"city\":\"beijing\",\"ext\":\"ext-bj\"}";
        String json = "{\"status\":200,\"consumer_username\":\"editor-web\",\"request\":\"POST /v1/car_audit/23432/6769729 HTTP/1.0\",\"bytes_sent\":231,\"fields\":{\"type\":\"editor_service_access\"},\"clientip\":\"10.46.181.38\",\"http_x_forwarded_for\":\"100.116.35.160, 10.46.181.38\",\"type\":\"EditingAccessLog\",\"log_id\":\"-\",\"timestamp\":\"2018-12-07 19:22:09\",\"cookie\":\"-\",\"elapsed\":0.008,\"hostname\":\"172.20.189.161\",\"http_user_agent\":\"okhttp/3.3.1\",\"source\":\"/mnt/logs/nginx/access-2018-12-07.log\",\"referrer\":\"-\",\"consumer_id\":\"39a2d0c4-abd1-40ea-9449-f91e6e449ac7\",\"trace_id\":\"-\",\"spy_id\":\"-\",\"request_body\":\"-\"}";
//        String json = "{\"schema\":{\"type\":\"struct\",\"fields\":[{\"type\":\"int64\",\"optional\":false,\"field\":\"id\"}],\"optional\":false,\"name\":\"jarvis_kafka_test03.jarvis_dev.sub_task.Key\"},\"payload\":{\"id\":718}}";
        JsonNode jsonNode = mapper.readTree(json);
        Iterator<String> keys = jsonNode.fieldNames();
        while (keys.hasNext()) {
            String fieldName = keys.next();
            JsonNode nodeValue = jsonNode.get(fieldName);
            if (nodeValue instanceof ContainerNode) {
                System.out.println(fieldName + ": " +nodeValue.toString());
            } else {
                System.out.println(fieldName + ": " + jsonNode.get(fieldName));
            }
        }
    }

    @Test
    public void jdbcConnectorTest() throws SQLException {
        JdbcConnectConfig connectConfig = JdbcConnectConfig.initCkConnectConfig("tdp01,tdp02,tdp03", "8123", "", "");
        JdbcDataSource dataSource = new JdbcDataSource(connectConfig);

        Map<String, String> colType = dataSource.descTableColType("bigdata.user_01");
        for (Map.Entry<String, String> entry : colType.entrySet()) {
            System.out.println(entry.getKey() + " : " + entry.getValue());
        }
    }

    @Test
    public void escapeTest() {
        String str = "ANR+Input+dispatching+'timed+out+(Waiting+because+the+touched+window\'s+input+channel+is+full.++Outbound+queue+length:+1.++Wait+queue+length:+52.";

        System.out.println(str.replaceAll("'", "\\\\'"));
    }

    @Test
    public void dateFormat() {

        SimpleDateFormat sdf = new SimpleDateFormat("qqqqqq");
        String format = sdf.format(new Date());

        System.out.println(format);
    }

}
