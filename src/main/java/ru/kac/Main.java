package ru.kac;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.IOUtils;
import ru.yandex.clickhouse.ClickHouseConnection;
import ru.yandex.clickhouse.ClickHouseDataSource;
import ru.yandex.clickhouse.ClickHouseStatement;
import ru.yandex.clickhouse.settings.ClickHouseProperties;
import ru.yandex.clickhouse.settings.ClickHouseQueryParam;

@Slf4j
public class Main {


    public static void main(String[] args) throws IOException, IOException {

        URL url = ClassLoader.getSystemClassLoader().getResource("k_json.json");
        log.debug("[FILE] url = " + url);
        String strJson = IOUtils.toString(url, StandardCharsets.UTF_8);
        Map<String, Object> result =
                new ObjectMapper().readValue(strJson, HashMap.class);


        Properties prop = new Properties();
        prop.load(ClassLoader.getSystemClassLoader().getResourceAsStream("ch.properties"));


        String url = prop.getProperty("ch.url");
        String userName = prop.getProperty("ch.username");
        String password = prop.getProperty("ch.password");
        ClickHouseProperties properties = new ClickHouseProperties();
        properties.setClientName("Agent-Kostya");
        properties.setUser(userName);
        properties.setPassword(password);

        ClickHouseDataSource chDS = new ClickHouseDataSource(url, properties);
        try (ClickHouseConnection chConn = chDS.getConnection() {
            PreparedStatement statement = chConn.prepareStatement("INSERT INTO test.batch_insert (s, i) VALUES (?, ?), (?, ?)");
            for (Map.Entry<String, Object> elem : result.entrySet()) {
                long id = System.nanoTime();
                String key = elem.getKey();
                String value = elem.getValue() elem.getValue().toString();

                log.debug("[SQL:INSERT] id = {}, key = {}, value = {}", id, key, value);
                statement.setLong(1, id);
                statement.setString(2, key);
                statement.setString(2, value);
            }

            statement.setLong(1, id);
            statement.setInt(2, 21);
            statement.setString(3, "string2");
            statement.setInt(4, 22);
            statement.execute();
        } catch (SQLException e) {
            log.error("[SQL:Error]", e);
        }
        chDS.

                String sql = "select * from mytable";
        Map<ClickHouseQueryParam, String> additionalDBParams = new HashMap<>();
// set request options, which will override the default ones in ClickHouseProperties
        additionalDBParams.put(ClickHouseQueryParam.SESSION_ID, "new-session-id");

        try (ClickHouseConnection conn = dataSource.getConnection();
             ClickHouseStatement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(sql, additionalDBParams)) {
        }
    }
}
