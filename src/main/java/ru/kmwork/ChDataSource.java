package ru.kmwork;

import cc.blynk.clickhouse.ClickHouseConnection;
import cc.blynk.clickhouse.ClickHouseDataSource;
import cc.blynk.clickhouse.settings.ClickHouseProperties;
import lombok.SneakyThrows;

public class ChDataSource {
    private ClickHouseDataSource chDS;

    private void loadDS() {
        ClickHouseProperties chProperties = ChUtils.loadClickHouseProperties();
        chDS = new ClickHouseDataSource(ChUtils.getUrl(), chProperties);
    }

    public ChDataSource() {
        loadDS();
    }

    @SneakyThrows
    public ClickHouseConnection getConnection() {
        return chDS.getConnection();
    }
}
