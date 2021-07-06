package ru.kmwork;

import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.Properties;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.IOUtils;
import org.junit.platform.commons.util.StringUtils;

@Slf4j
public class AppUtils {

    private static final String APP_PROFILE = "app.profile";

    private AppUtils() {
    }

    @SneakyThrows
    public static Properties loadProperties(String fileName) {
        String profile = System.getProperty(APP_PROFILE);
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
        log.info("[Read:File] Чтение ресурса : " + url);
        prop.load(ClassLoader.getSystemClassLoader().getResourceAsStream(url));
        for (Map.Entry<Object, Object> p : prop.entrySet()) {
            log.debug("[Properties: {}] [Param: {}] = {}", fileName, p.getKey(), p.getValue());
        }
        return prop;
    }

    @SneakyThrows
    public static String readJson(String fileName) {
        URL jsonUrl = ClassLoader.getSystemClassLoader().getResource(fileName);
        log.debug("[Read:FILE] url = " + jsonUrl);
        return IOUtils.toString(jsonUrl, StandardCharsets.UTF_8);
    }
}
