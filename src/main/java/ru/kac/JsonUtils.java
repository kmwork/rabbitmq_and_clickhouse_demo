package ru.kac;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.Map;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.SneakyThrows;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class JsonUtils {

    private static final int DEFAULT_MAP_SIZE = 128;

    private JsonUtils() {
    }

    @SneakyThrows
    public static Map<String, JsonUtils.IndexedValue> jsonToMap(String strJson) {
        ObjectMapper objectMapper = new ObjectMapper();
        Map<String, JsonUtils.IndexedValue> result = transformJsonToMapIterative(objectMapper.readTree(strJson));

        if (log.isTraceEnabled()) {
            log.trace("[Original-Json] {}", strJson);
            for (Map.Entry<String, JsonUtils.IndexedValue> en : result.entrySet()) {
                log.trace("[Item: {}] = {}", en.getKey(), en.getValue());
            }
        }
        return result;
    }

    @AllArgsConstructor
    @ToString
    private static class JsonNodeWrapper {
        private Integer index;
        private JsonNode node;
        private String prefix;

        public String getKey() {
            StringBuilder sb = new StringBuilder();
            sb.append(prefix);
            if (index != null) sb.append("[").append(index).append("]");
            return sb.toString();

        }
    }

    @AllArgsConstructor
    @ToString
    public static class IndexedValue {
        @Getter
        private Integer index;
        @Getter
        private String prefix;
        @Getter
        private String valueAsText;
    }

    //------------ Transform jackson JsonNode to Map -Iterative version -----------
    private static Map<String, IndexedValue> transformJsonToMapIterative(JsonNode node) {
        Map<String, IndexedValue> jsonMap = new LinkedHashMap<>(DEFAULT_MAP_SIZE);
        LinkedList<JsonNodeWrapper> queue = new LinkedList<>();

        //Add root of json tree to Queue
        JsonNodeWrapper root = new JsonNodeWrapper(null, node, "");
        queue.offer(root);

        while (!queue.isEmpty()) {
            JsonNodeWrapper curElement = queue.poll();
            if (curElement.node.isObject()) {
                //Add all fields (JsonNodes) to the queue
                Iterator<Map.Entry<String, JsonNode>> fieldIterator = curElement.node.fields();
                while (fieldIterator.hasNext()) {
                    Map.Entry<String, JsonNode> field = fieldIterator.next();
                    String prefix = (curElement.prefix == null || curElement.prefix.trim().length() == 0) ? "" : curElement.prefix + ".";
                    queue.offer(new JsonNodeWrapper(null, field.getValue(), prefix + field.getKey()));
                }
            } else if (curElement.node.isArray()) {
                //Add all array elements(JsonNodes) to the Queue
                int i = 0;
                for (JsonNode arrayElement : curElement.node) {
                    queue.offer(new JsonNodeWrapper(i, arrayElement, curElement.prefix));
                    i++;
                }
            } else {
                //If basic type, then time to fetch the Property value
                jsonMap.put(curElement.getKey(), new IndexedValue(curElement.index, curElement.prefix, curElement.node.asText()));

            }
        }
        return jsonMap;
    }
}
