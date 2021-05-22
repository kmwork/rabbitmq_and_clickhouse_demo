package ru.kac;

import java.util.Map;
import java.util.Properties;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.junit.platform.commons.util.StringUtils;

@Slf4j
public class AppUtils {

    private static String APP_PROFILE = "app.profile";

    @SneakyThrows
    public static Properties loadProperties(String fileName) {
        String profile = System.getProperty("app.profile");
        if (StringUtils.isBlank(profile)) {
            log.error("Не указано свойство java options '{}'. Возможные значение: kostya, demo", APP_PROFILE);
            System.exit(-10);
        }

        Properties prop = new Properties();
        StringBuilder sb = new StringBuilder();
        sb.append(profile).append("-profile");
        sb.append("/");
        sb.append(profile).append("-").append(fileName);
        String url = sb.toString();
        log.info("[File] Чтение ресурса : " + url);
        prop.load(ClassLoader.getSystemClassLoader().getResourceAsStream(url));
        for (Map.Entry<Object, Object> p : prop.entrySet()) {
            log.debug("[Properties: {}] [Param: {}] = {}", fileName, p.getKey(), p.getValue());
        }
        return prop;
    }
}
