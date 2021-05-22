package ru.kac;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.Map;
import lombok.NoArgsConstructor;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@NoArgsConstructor
public class DoSimpleJsonApp {

    private int index;

    @SneakyThrows
    public static void main(String[] args) {
        DoSimpleJsonApp app = new DoSimpleJsonApp();
        app.run();
    }

    @SneakyThrows
    private void run() {
        String strJson = AppUtils.readJson("k_json.json");
        ObjectMapper objectMapper = new ObjectMapper();
        Map<String, String> propsIterative = JsonUtils.jsonToMap(strJson);

        log.debug("[Original-Json] {}", strJson);

        for (Map.Entry<String, String> en : propsIterative.entrySet()) {
            log.debug("[Item: {}] = {}", en.getKey(), en.getValue());
        }
    }
}
