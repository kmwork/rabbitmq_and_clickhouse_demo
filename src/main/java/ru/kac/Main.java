package ru.kac;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.TimeZone;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.IOUtils;
import ru.yandex.clickhouse.ClickHouseConnection;
import ru.yandex.clickhouse.ClickHouseDataSource;
import ru.yandex.clickhouse.settings.ClickHouseProperties;

@Slf4j
public class Main {


    public static void main(String[] args) throws IOException, IOException {

        URL jsonUrl = ClassLoader.getSystemClassLoader().getResource("k_json.json");
        log.debug("[FILE] url = " + jsonUrl);
        String strJson = IOUtils.toString(jsonUrl, StandardCharsets.UTF_8);
        Map<String, Object> result =
                new ObjectMapper().readValue(strJson, HashMap.class);


        Properties prop = new Properties();
        prop.load(ClassLoader.getSystemClassLoader().getResourceAsStream("ch.properties"));


        String chUrl = prop.getProperty("ch.url");
        String chUserName = prop.getProperty("ch.username");
        String chPassword = prop.getProperty("ch.password");
        String chTable = prop.getProperty("ch.table");
        String chTimeZone = TimeZone.getDefault().toZoneId().getId();
        ClickHouseProperties chProperties = new ClickHouseProperties();
        chProperties.setUseServerTimeZone(false);
        chProperties.setUseTimeZone(chTimeZone);
        chProperties.setClientName("Agent-Kostya");
        chProperties.setUser(chUserName);
        chProperties.setPassword(chPassword);
        chProperties.setClientName("Agent #1");
        chProperties.setSessionId("default-session-id");

        ClickHouseDataSource chDS = new ClickHouseDataSource(chUrl, chProperties);
        try (ClickHouseConnection chConn = chDS.getConnection()) {

            StringBuilder sqlBuilder = new StringBuilder();
            sqlBuilder.append("INSERT INTO ").append(chTable);
            sqlBuilder.append(".batch_insert ").append("(id, key,value) VALUES ");
            for (int i = 0; i < result.size(); i++) {
                if (i > 0)
                    sqlBuilder.append(", ");
                sqlBuilder.append("(?, ?,?)");
            }
            String sql = sqlBuilder.toString();
            log.debug("[SQL] sql = " + sql);

            PreparedStatement statement = chConn.prepareStatement(sql);
            for (Map.Entry<String, Object> elem : result.entrySet()) {
                long id = System.nanoTime();
                String key = elem.getKey();
                String value = elem.getValue() == null ? null : elem.getValue().toString();

                log.debug("[SQL:INSERT] id = {}, key = {}, value = {}", id, key, value);
                statement.setLong(1, id);
                statement.setString(2, key);
                statement.setString(2, value);
                statement.execute();
            }

        } catch (SQLException e) {
            log.error("[SQL:Error]", e);
        }

//        Map<ClickHouseQueryParam, String> additionalDBParams = new HashMap<>();
//// set request options, which will override the default ones in ClickHouseProperties
//        additionalDBParams.put(ClickHouseQueryParam.SESSION_ID, "new-session-id");
//
//        try (ClickHouseConnection conn = dataSource.getConnection();
//             ClickHouseStatement stmt = conn.createStatement();
//             ResultSet rs = stmt.executeQuery(sql, additionalDBParams)) {
//        }
    }
}
