package ru.kac;

import cc.blynk.clickhouse.settings.ClickHouseProperties;
import java.util.Properties;
import java.util.TimeZone;
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
        sqlBuilder.append("(id,key,value) FORMAT Values ");
        for (int i = 0; i < rowCount; i++) {
            if (i > 0)
                sqlBuilder.append(", ");
            sqlBuilder.append("(?,?,?)");
        }
        String sql = sqlBuilder.toString();
        if (log.isTraceEnabled()) {
            log.trace("[SQL] sql = " + sql);
        }
        return sql;
    }

}
