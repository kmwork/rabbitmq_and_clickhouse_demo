package ru.kac;

import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class MqUtils {

    private static final Properties prop = new Properties();

    static {
        loadProp();
    }

    @SneakyThrows
    private static void loadProp() {
        prop.load(ClassLoader.getSystemClassLoader().getResourceAsStream("rabbitmq.properties"));
        log.debug("[System:MQ-Properties] prop =" + prop);
    }

    @SneakyThrows
    public static Connection createMqConnection() {
        String mqHost = prop.getProperty("mq.host");
        String mqVirtualHost = prop.getProperty("mq.virtualHost");
        String mqUserName = prop.getProperty("mq.username");
        String mqPassword = prop.getProperty("mq.password");

        ConnectionFactory cf = new ConnectionFactory();
        cf.setHost(mqHost);
        cf.setVirtualHost(mqVirtualHost);
        cf.setUsername(mqUserName);
        cf.setPassword(mqPassword);
        return cf.newConnection();
    }

    public static String getQueue() {
        return prop.getProperty("mq.queue");
    }

    public static Map<String, Object> getMqArguments() {
        Map<String, Object> arguments = new HashMap<>();
        arguments.put("x-max-length", Integer.valueOf(prop.getProperty("mq.xMaxLength")));
        arguments.put("reject-publish", prop.getProperty("mq.rejectPublish"));
        arguments.put("x-overflow", prop.getProperty("mq.xOverflow"));

        return arguments;
    }
}
