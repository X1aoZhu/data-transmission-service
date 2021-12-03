package com.zhu.dts.source;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.junit.Test;

/**
 * @Author ZhuHaiBo
 * @Create 2021/12/2 22:04
 */
public class KafkaSourceTest {

    private final static String BOOTSTRAP_SERVER = "192.168.240.155:9092";

    private final static String DEBEZIUM_JSON_FORMAT = "debezium-json";

    private final StreamExecutionEnvironment env =
            StreamExecutionEnvironment.getExecutionEnvironment();

    private final StreamTableEnvironment tEnv =
            StreamTableEnvironment.create(
                    env,
                    EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build());

    @Test
    public void testReadKafka() throws Exception {
        String productTopic = "ods_product_dts";

        String productSQL = String.format(
                "create table product(\n" +
                        "    id int,\n" +
                        "    title String,\n" +
                        "    cid int,\n" +
                        "    category_name String,\n" +
                        "    weight int,\n" +
                        "    price int,\n" +
                        "    PRIMARY KEY(id) NOT ENFORCED\n" +
                        ")with(\n" +
                        "    'connector' = 'kafka',\n" +
                        "    'topic' = '%s',\n" +
                        "    'properties.bootstrap.servers' = '%s',\n" +
                        "    'format' = '%s',\n" +
                        "    'scan.startup.mode' = 'earliest-offset'\n" +
                        ")", productTopic, BOOTSTRAP_SERVER, DEBEZIUM_JSON_FORMAT
        );

        tEnv.executeSql(productSQL);
        tEnv.executeSql("show tables").print();
        tEnv.executeSql("select * from product").print();
        //Table table = tEnv.sqlQuery("select * from product");
        //tEnv.toRetractStream(table, Row.class).print("SQL Result");
        //tEnv.toChangelogStream(table).print("SQL Result");
    }

}
