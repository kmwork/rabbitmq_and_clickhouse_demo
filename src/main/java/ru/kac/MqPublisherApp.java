package ru.kac;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.ConfirmCallback;
import com.rabbitmq.client.Connection;
import java.time.Duration;
import java.util.Map;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.function.BooleanSupplier;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class MqPublisherApp {

    static final int MESSAGE_COUNT = 7_000;


    public static void main(String[] args) throws Exception {
        publishMessagesInBatch();
    }

    static void publishMessagesInBatch() throws Exception {
        try (Connection connection = MqUtils.createMqConnection()) {
            Channel ch = connection.createChannel();
            String queue = MqUtils.getQueue();
            Map<String, Object> arguments = MqUtils.getMqArguments();
            ch.queueDeclare(queue, true, false, false, arguments);

            ch.confirmSelect();

            int batchSize = 100;

            ConcurrentNavigableMap<Long, String> outstandingConfirms = new ConcurrentSkipListMap<>();

            ConfirmCallback cleanOutstandingConfirms = (sequenceNumber, multiple) -> {
                if (multiple) {
                    ConcurrentNavigableMap<Long, String> confirmed = outstandingConfirms.headMap(
                            sequenceNumber, true
                    );
                    confirmed.clear();
                } else {
                    outstandingConfirms.remove(sequenceNumber);
                }
            };

            ch.addConfirmListener(cleanOutstandingConfirms, (sequenceNumber, multiple) -> {
                String body = outstandingConfirms.get(sequenceNumber);
                String msg = String.format(
                        "Message with body %s has been nack-ed. Sequence number: %d, multiple: %b%n",
                        body, sequenceNumber, multiple
                );
                log.error(msg);
                cleanOutstandingConfirms.handle(sequenceNumber, multiple);
            });


            long start = System.nanoTime();
            for (int i = 0; i < MESSAGE_COUNT; i++) {
                long value = System.nanoTime();


                String body = String.format("{\n" +
                        "\"key\": %d,\n" +
                        "\"value\": %d \n" +
                        "}", value, value);
                outstandingConfirms.put(ch.getNextPublishSeqNo(), body);
                ch.basicPublish("", queue, null, body.getBytes());
            }


            if (!waitUntil(Duration.ofSeconds(60), () -> outstandingConfirms.isEmpty())) {
                throw new IllegalStateException("All messages could not be confirmed in 60 seconds");
            }


            long end = System.nanoTime();
            String msq = String.format("Published %,d messages in batch in %,d ms%n", MESSAGE_COUNT, Duration.ofNanos(end - start).toMillis());
            log.info(msq);
        }
    }


    static boolean waitUntil(Duration timeout, BooleanSupplier condition) throws InterruptedException {
        int waited = 0;
        while (!condition.getAsBoolean() && waited < timeout.toMillis()) {
            Thread.sleep(100L);
            waited = +100;
        }
        return condition.getAsBoolean();
    }


}
