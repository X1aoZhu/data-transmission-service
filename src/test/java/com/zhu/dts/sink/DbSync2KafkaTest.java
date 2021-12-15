package com.zhu.dts.sink;

import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.zhu.dts.debezium.StringDebeziumDeserializationSchema;
import com.zhu.dts.kafka.StringKafkaSerializationSchema;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.kafka.shaded.org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.junit.Test;

import java.util.Properties;

/**
 * @Author ZhuHaiBo
 * @Create 2021/12/7 11:59
 */
public class DbSync2KafkaTest {

    private final static StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

    private static final String DEFAULT_TOPIC = "default_topic";

    private static final String TABLE_NAME = "cnarea_2020";

    private static final String BOOTSTRAP_SERVER = "192.168.240.155:9092";

    @Test
    public void sinkTest() {
        long beginTime = System.currentTimeMillis();
        // MysqlSource
        MySqlSource<String> mySqlSource = MySqlSource.<String>builder()
                .hostname("192.168.240.155")
                .port(3306)
                .databaseList("flink_cdc")
                .tableList(TABLE_NAME)
                .username("root")
                .password("root")
                .deserializer(new StringDebeziumDeserializationSchema())
                .serverTimeZone("Asia/Shanghai")
                .startupOptions(StartupOptions.initial())
                .build();

        // KafkaSink
        Properties kafkaProperties = new Properties();
        kafkaProperties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVER);
        kafkaProperties.put(ProducerConfig.ACKS_CONFIG, "-1");
        kafkaProperties.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);


        FlinkKafkaProducer<String> kafkaProducer = new FlinkKafkaProducer<>(DEFAULT_TOPIC,
                new StringKafkaSerializationSchema(), kafkaProperties, FlinkKafkaProducer.Semantic.AT_LEAST_ONCE);

        // enable checkpoint
        env.enableCheckpointing(3000L);

        //env.fromSource(mySqlSource, WatermarkStrategy.noWatermarks(), "MySQL Source")
        //        .name("mysqlcdc-source")
        //        .uid("uid-mysqlcdc-source")
        //        .addSink(kafkaProducer).name("kafka-sink").uid("kafka-sink");

        env.fromSource(mySqlSource, WatermarkStrategy.noWatermarks(), "MySQL Source").print();
    }
}