package ru.kac;

import cc.blynk.clickhouse.ClickHouseConnection;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.DeliverCallback;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;
import lombok.NoArgsConstructor;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@NoArgsConstructor
public class ChFromRabbitMQApp {

    private static final AtomicInteger successCount = new AtomicInteger(0);
    private static final AtomicInteger totalCount = new AtomicInteger(0);
    private static final AtomicInteger errorCountJson = new AtomicInteger(0);
    private static final AtomicInteger errorCountSql = new AtomicInteger(0);
    private static final long SLEEP_MS = 1000;

    private final ChDataSource chDataSource = new ChDataSource();
    private static final int NUM_LOOP = 1000;

    enum AppTypeError {
        OK,
        ERROR_SQL,
        INVALID_JSON
    }

    static {
        Runtime.getRuntime().addShutdownHook(new ChHook());
    }


    static class ChHook extends Thread {
        public void run() {
            log.info("[FINISH-APP] successCount = {}, totalCount = {}", successCount, totalCount);
            if (errorCountJson.get() > 0 || errorCountSql.get() > 0) {
                log.error("[App:Errors] errorCountSql = {}, errorCountJson = {}" + errorCountSql, errorCountJson);
            } else log.info("[FINISH-APP] Нет ошибок");
        }
    }


    @SneakyThrows
    public static void main(String[] args) {
        ChFromRabbitMQApp demoApp = new ChFromRabbitMQApp();
        demoApp.run();

    }


    @SneakyThrows
    public void run() {
        try (Connection connection = MqUtils.createMqConnection()) {
            Channel channel = connection.createChannel();

            String queue = MqUtils.getQueue();
            Map<String, Object> arguments = MqUtils.getMqArguments();
            channel.queueDeclare(queue, true, false, false, arguments);


            log.info(" [*] Waiting for messages. To exit press CTRL+C");

            DeliverCallback deliverCallback = (consumerTag, delivery) -> {
                String strJson = new String(delivery.getBody(), "UTF-8");
                log.info(" [x] Received '" + strJson + "'");
                AppTypeError typeError = addMqToCh(strJson);
                switch (typeError) {
                    case ERROR_SQL:
                        errorCountSql.incrementAndGet();
                        break;
                    case INVALID_JSON:
                        errorCountJson.incrementAndGet();
                        break;
                    case OK:
                        successCount.incrementAndGet();
                        break;
                }
                totalCount.incrementAndGet();


            };

            for (int i = 0; i < NUM_LOOP; i++) {
                channel.basicConsume(queue, true, deliverCallback, consumerTag -> {
                });
                Thread.sleep(SLEEP_MS);
            }
        }


    }

    @SneakyThrows
    private AppTypeError addMqToCh(String strJson) {
        Map<String, Object> result = null;
        if (strJson == null) {
            return AppTypeError.INVALID_JSON;
        }
        try {
            result =
                    new ObjectMapper().readValue(strJson, HashMap.class);
        } catch (JsonProcessingException e) {
            log.error("[Error:Json-Invalid] strJson = " + strJson);
            return AppTypeError.INVALID_JSON;
        }

        Properties prop = new Properties();
        prop.load(ClassLoader.getSystemClassLoader().getResourceAsStream("kostya-profile/kostya-ch.properties"));


        try (ClickHouseConnection chConn = chDataSource.getConnection()) {

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
            return AppTypeError.ERROR_SQL;
        }
        return AppTypeError.OK;

    }
}
