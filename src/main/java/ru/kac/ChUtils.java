package ru.kac;

import cc.blynk.clickhouse.settings.ClickHouseProperties;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Map;
import java.util.Properties;
import java.util.TimeZone;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class ChUtils {

    private static final Properties prop = AppUtils.loadProperties("ch.properties");

    private ChUtils() {
    }

    public static ClickHouseProperties loadClickHouseProperties() {
        String chUserName = prop.getProperty("ch.username");
        String chPassword = prop.getProperty("ch.password");
        String chTimeZone = TimeZone.getDefault().toZoneId().getId();
        ClickHouseProperties chProperties = new ClickHouseProperties();
        chProperties.setUseServerTimeZone(false);
        chProperties.setUseTimeZone(chTimeZone);
        chProperties.setUser(chUserName);
        chProperties.setPassword(chPassword);
        chProperties.setSessionId("default-session-id");

        return chProperties;
    }

    public static String getUrl() {
        return prop.getProperty("ch.url");
    }

    public static String getSchema() {
        return prop.getProperty("ch.schema");
    }


    public static String getTable() {
        return prop.getProperty("ch.table");
    }

    public static String insertBatch(int rowCount) {
        StringBuilder sqlBuilder = new StringBuilder();
        sqlBuilder.append("INSERT INTO ").append(ChUtils.getSchema()).append(".").append(ChUtils.getTable()).append(" ");
        sqlBuilder.append("(id,key,value,array_index) FORMAT Values ");
        for (int i = 0; i < rowCount; i++) {
            if (i > 0)
                sqlBuilder.append(", ");
            sqlBuilder.append("(?,?,?,?)");
        }
        String sql = sqlBuilder.toString();
        if (log.isTraceEnabled()) {
            log.trace("[SQL] sql = " + sql);
        }
        return sql;
    }

    @SneakyThrows
    public static ChFromRabbitMQApp.AppTypeError addMqToCh(Connection chConn, String strJson) {
        if (strJson == null) {
            return ChFromRabbitMQApp.AppTypeError.INVALID_JSON;
        }

        Map<String, JsonUtils.IndexedValue> result;
        try {
            result = JsonUtils.jsonToMap(strJson);
        } catch (Exception e) {
            log.error("[Error:Json-Invalid] strJson = " + strJson);
            return ChFromRabbitMQApp.AppTypeError.INVALID_JSON;
        }

        try (chConn) {

            String sql = insertBatch(result.size());
            PreparedStatement statement = chConn.prepareStatement(sql);

            int sqlParamIndex = 0;
            long id = System.nanoTime();
            for (Map.Entry<String, JsonUtils.IndexedValue> elem : result.entrySet()) {
                JsonUtils.IndexedValue value = elem.getValue();

                if (log.isTraceEnabled()) {
                    log.trace("[SQL:INSERT] id = {}, key = {}, value = {}, index = {}", id, value.getPrefix(), value, value.getIndex());
                }
                statement.setLong(++sqlParamIndex, id);
                statement.setString(++sqlParamIndex, value.getPrefix());
                statement.setString(++sqlParamIndex, value.getValueAsText());
                statement.setInt(++sqlParamIndex, value.getIndex() == null ? -1 : value.getIndex());

            }
            statement.execute();
        } catch (SQLException e) {
            log.error("[SQL:Error]", e);
            return ChFromRabbitMQApp.AppTypeError.ERROR_SQL;
        }
        return ChFromRabbitMQApp.AppTypeError.OK;

    }

}
