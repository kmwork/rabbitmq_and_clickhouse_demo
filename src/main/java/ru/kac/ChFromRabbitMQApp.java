package ru.kac;

import cc.blynk.clickhouse.ClickHouseConnection;
import cc.blynk.clickhouse.ClickHouseDataSource;
import cc.blynk.clickhouse.settings.ClickHouseProperties;
import com.fasterxml.jackson.databind.ObjectMapper;
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
public class ChFromRabbitMQApp {

    private final ClickHouseProperties clickHouseProperties;

    public ChFromRabbitMQApp() {
        clickHouseProperties = ChUtils.loadClickHouseProperties();
    }


    @SneakyThrows
    public static void main(String[] args) {
        ChFromRabbitMQApp demoApp = new ChFromRabbitMQApp();
        demoApp.run();
    }

    @SneakyThrows
    public void run() {
        ChFromRabbitMQApp demoApp = new ChFromRabbitMQApp();

        URL jsonUrl = ClassLoader.getSystemClassLoader().getResource("k_json.json");
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
