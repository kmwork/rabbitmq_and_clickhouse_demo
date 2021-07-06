package ru.kmwork;


import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.DeliverCallback;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import lombok.NoArgsConstructor;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@NoArgsConstructor
public class RabbitMQReadApp {



    @SneakyThrows
    public void run() {
        try (Connection connection = MqUtils.createMqConnection(); Channel channel = connection.createChannel()) {

            String queue = MqUtils.getQueue();
            Map<String, Object> arguments = MqUtils.getMqArguments();
            channel.queueDeclare(queue, true, false, false, arguments);


            log.info(" [*] Waiting for messages. To exit press CTRL+C");

            DeliverCallback deliverCallback = (consumerTag, delivery) -> {
                String message = new String(delivery.getBody(), StandardCharsets.UTF_8);
                if (log.isTraceEnabled()) {
                    log.trace(" [x] Received '" + message + "'");
                }
            };
            channel.basicConsume(queue, true, deliverCallback, consumerTag -> {
            });
        }
    }


    @SneakyThrows
    public static void main(String[] args) {
        RabbitMQReadApp demoApp = new RabbitMQReadApp();
        demoApp.run();
    }

}
