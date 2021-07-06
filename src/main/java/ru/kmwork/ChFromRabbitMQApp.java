package ru.kmwork;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.DeliverCallback;
import java.nio.charset.StandardCharsets;
import java.util.Map;
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
    private static final long SLEEP_MS = 100;

    private final ChDataSource chDataSource = new ChDataSource();
    private static final long NUM_LOOP = Long.MAX_VALUE; //1000;

    enum AppTypeError {
        OK,
        ERROR_SQL,
        INVALID_JSON
    }

    static {
        Runtime.getRuntime().addShutdownHook(new ChHook());
    }

    static class ChHook extends Thread {
        @Override
        public void run() {
            log.info("[FINISH-APP] successCount = {}, totalCount = {}", successCount, totalCount);
            if (errorCountJson.get() > 0 || errorCountSql.get() > 0) {
                log.error("[App:Errors] errorCountSql = {}, errorCountJson = {}", errorCountSql, errorCountJson);
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
        try (Connection connection = MqUtils.createMqConnection(); Channel channel = connection.createChannel()) {

            String queue = MqUtils.getQueue();
            Map<String, Object> arguments = MqUtils.getMqArguments();
            channel.queueDeclare(queue, true, false, false, arguments);


            log.info(" [*] Waiting for messages. To exit press CTRL+C");

            DeliverCallback deliverCallback = (consumerTag, delivery) -> {
                String strJson = new String(delivery.getBody(), StandardCharsets.UTF_8);
                if (log.isTraceEnabled()) {
                    log.trace(" [x] Received '" + strJson + "'");
                }
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
                int currentCount = totalCount.incrementAndGet();
                if (currentCount % 200 == 0) {
                    log.info("[MQ] Прочитано {} сообщений", currentCount);
                }
            };

            for (int i = 0; i < NUM_LOOP; i++) {
                channel.basicConsume(queue, true, deliverCallback, consumerTag -> {
                });
                log.trace("[Sleep] on ms = " + SLEEP_MS);
                Thread.sleep(SLEEP_MS);
            }
        }
    }

    @SneakyThrows
    private AppTypeError addMqToCh(String strJson) {
        return ChUtils.addMqToCh(chDataSource.getConnection(), strJson);
    }


}
