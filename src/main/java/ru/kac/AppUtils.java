package ru.kac;

import java.util.Map;
import java.util.Properties;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class AppUtils {

    @SneakyThrows
    public static Properties loadProperties(String fileName) {
        Properties prop = new Properties();
        prop.load(ClassLoader.getSystemClassLoader().getResourceAsStream(fileName));
        for (Map.Entry<Object, Object> p : prop.entrySet()) {
            log.debug("[Properties: {}] [Param: {}] = {}", fileName, p.getKey(), p.getValue());
        }
        return prop;
    }
}
