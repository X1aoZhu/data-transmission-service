package com.zhu.dts.core;

import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.zhu.dts.debezium.MyDebeziumDeserializationSchema;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.kafka.shaded.org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import com.zhu.dts.entity.ParameterEntity;
import com.zhu.dts.kafka.MyKafkaSerializationSchema;
import com.zhu.dts.util.SystemConfigUtil;

import java.util.Properties;

/**
 * @Author ZhuHaiBo
 * @Create 2021/11/30 1:01
 */
public class Sync2Kafka {

    private static final String DEFAULT_TOPIC = "default_topic";

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 获取启动参数
        ParameterTool parameters = ParameterTool.fromArgs(args);
        ParameterEntity configEntity = SystemConfigUtil.convert(parameters);

        // MysqlSource
        MySqlSource<String> mySqlSource = MySqlSource.<String>builder()
                .hostname(configEntity.getConfigHostname())
                .port(Integer.parseInt(configEntity.getConfigPort()))
                .databaseList(configEntity.getConfigDatabaseName())
                .tableList(configEntity.getConfigTableList())
                .username(configEntity.getConfigUsername())
                .password(configEntity.getConfigPassword())
                .deserializer(new MyDebeziumDeserializationSchema())
                .serverTimeZone("Asia/Shanghai")
                .startupOptions(StartupOptions.initial())
                .build();

        // KafkaSink
        Properties kafkaProperties = new Properties();
        kafkaProperties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, configEntity.getConfigKafkaBootstrapServer());
        kafkaProperties.put(ProducerConfig.ACKS_CONFIG, "-1");
        kafkaProperties.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);


        FlinkKafkaProducer<String> kafkaProducer = new FlinkKafkaProducer<>(DEFAULT_TOPIC,
                new MyKafkaSerializationSchema(Boolean.toString(configEntity.isMetadataFilter())), kafkaProperties, FlinkKafkaProducer.Semantic.AT_LEAST_ONCE);

        // enable checkpoint
        env.enableCheckpointing(configEntity.getConfigCheckpointInterval());

        env.fromSource(mySqlSource, WatermarkStrategy.noWatermarks(), "MySQL Source")
                .name("mysqlcdc-source")
                .uid("uid-mysqlcdc-source")
                .addSink(kafkaProducer).name("kafka-sink").uid("kafka-sink");

        //mysqlSource.addSink(kafkaProducer).name("kafka-sink").uid("kafka-sink");

        env.execute("Print MySQL Snapshot + Binlog");
    }
}
