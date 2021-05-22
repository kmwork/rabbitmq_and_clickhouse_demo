package ru.kac;

import cc.blynk.clickhouse.ClickHouseConnection;
import cc.blynk.clickhouse.ClickHouseDataSource;
import cc.blynk.clickhouse.settings.ClickHouseProperties;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.IOUtils;

@Slf4j
public class ChMainFromRabbitMQ {

    private final ClickHouseProperties clickHouseProperties;

    private Connection createRabbitConnection() throws Exception {
        ConnectionFactory cf = new ConnectionFactory();
        cf.setHost("localhost");
        cf.setVirtualHost("vhost_ch");
        cf.setUsername("for_ch_root");
        cf.setPassword("1");
        return cf.newConnection();
    }

    public ChMainFromRabbitMQ() {
        clickHouseProperties = ChUtils.loadClickHouseProperties();
    }


    @SneakyThrows
    public static void main(String[] args) {
        ChMainFromRabbitMQ demoApp = new ChMainFromRabbitMQ();
        demoApp.run();
    }

    @SneakyThrows
    public void run() {
        ChMainFromRabbitMQ demoApp = new ChMainFromRabbitMQ();
        demoApp.run();

        URL jsonUrl = ClassLoader.getSystemClassLoader().getResource("k_json.json");
        log.debug("[FILE] url = " + jsonUrl);
        String strJson = IOUtils.toString(jsonUrl, StandardCharsets.UTF_8);
        Map<String, Object> result =
                new ObjectMapper().readValue(strJson, HashMap.class);


        Properties prop = new Properties();
        prop.load(ClassLoader.getSystemClassLoader().getResourceAsStream("ch.properties"));


        ClickHouseDataSource chDS = new ClickHouseDataSource(ChUtils.getUrl(), clickHouseProperties);
        try (ClickHouseConnection chConn = chDS.getConnection()) {

            String sql = ChUtils.insertBatch(result.size());
            PreparedStatement statement = chConn.prepareStatement(sql);

            int sqlParamIndex = 0;
            long id = System.nanoTime();
            for (Map.Entry<String, Object> elem : result.entrySet()) {
                String key = elem.getKey();
                String value = elem.getValue() == null ? null : elem.getValue().toString();

                log.debug("[SQL:INSERT] id = {}, key = {}, value = {}", id, key, value);
                statement.setLong(++sqlParamIndex, id);
                statement.setString(++sqlParamIndex, key);
                statement.setString(++sqlParamIndex, value);

            }
            statement.execute();
        } catch (SQLException e) {
            log.error("[SQL:Error]", e);
        }

    }
}
