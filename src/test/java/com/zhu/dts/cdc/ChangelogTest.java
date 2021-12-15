package com.zhu.dts.cdc;

import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.connectors.mysql.source.MySqlSourceBuilder;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.ververica.cdc.debezium.StringDebeziumDeserializationSchema;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.data.RowData;
import org.junit.Test;

import java.time.ZoneId;

/**
 * @Author ZhuHaiBo
 * @Create 2021/12/15 1:13
 */
public class ChangelogTest {

    private final StreamExecutionEnvironment env =
            StreamExecutionEnvironment.getExecutionEnvironment();

    private final StreamTableEnvironment tEnv =
            StreamTableEnvironment.create(
                    env,
                    EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build());

    /**
     * table.exec.source.cdc-events-duplicate=true;
     *
     * 验证当前配置，是否只适用于changelog-json
     * 官方文档地址：
     * https://nightlies.apache.org/flink/flink-docs-release-1.14/zh/docs/connectors/table/formats/debezium/#%e9%87%8d%e5%a4%8d%e7%9a%84%e5%8f%98%e6%9b%b4%e4%ba%8b%e4%bb%b6
     */
    @Test
    public void configTest() throws Exception {

        MySqlSource<String> mySqlSource = MySqlSource.<String>builder()
                .hostname("192.168.240.155")
                .port(3306)
                .databaseList("flink_cdc")
                .tableList("flink_cdc.product")
                .username("root")
                .password("root")
                .deserializer(new StringDebeziumDeserializationSchema())
                .serverTimeZone("Asia/Shanghai")
                .startupOptions(StartupOptions.initial())
                .build();

        // enable checkpoint
        env.enableCheckpointing(3000);

        env.fromSource(mySqlSource, WatermarkStrategy.noWatermarks(), "MySQL Source")
                .setParallelism(4)
                .print().setParallelism(1);

        env.execute("Print MySQL Snapshot + Binlog");

    }

}
