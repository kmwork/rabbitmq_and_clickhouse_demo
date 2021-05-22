package ru.kac;

import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class ChInsertFromFileApp {

    @SneakyThrows
    public static void main(String[] args) {
        String strJson = AppUtils.readJson("k_json.json");
        final ChDataSource chDataSource = new ChDataSource();
        ChUtils.addMqToCh(chDataSource.getConnection(), strJson);
    }
}
