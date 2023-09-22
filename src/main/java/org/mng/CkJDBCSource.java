package org.mng;

import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.flink.types.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;


public class CkJDBCSource extends RichSourceFunction<Row> {
    private static final Logger LOG = LoggerFactory.getLogger(CkJDBCSource.class);
    private final String drivername = "ru.yandex.clickhouse.ClickHouseDriver";
    private  String url = "jdbc:clickhouse://60.204.155.55:8123/dataflow";
    private final String query;

    private volatile boolean isRunning = true;

    public CkJDBCSource(String query) {
        this.query = query;
    }

    @Override
    public void run(SourceContext<Row> sourceContext) throws Exception {
        final Properties properties = new Properties();
        properties.setProperty("driver", drivername);
        properties.setProperty("user", "default");
        properties.setProperty("password", "bdilab@1308");
        try (Connection conn = DriverManager.getConnection(url, properties);
            PreparedStatement ps = conn.prepareStatement(query)) {
            while (isRunning ) {
                try (ResultSet rs = ps.executeQuery()) {
                    int columnCount = rs.getMetaData().getColumnCount();
                    while (rs.next()) {
                        final Row row = new Row(columnCount);
                        for(int i = 0;i< columnCount;i++){
                            row.setField(i, rs.getObject(i+1));
                        }
                        sourceContext.collect(row);
                    }
                    Thread.sleep(1000);
                }
                isRunning = false;
            }
        } catch (Exception e) {
            LOG.error("Failed executing query {}", query, e);
            throw e;
        }
    }

    @Override
    public void cancel() {
        isRunning = false;
    }
}
