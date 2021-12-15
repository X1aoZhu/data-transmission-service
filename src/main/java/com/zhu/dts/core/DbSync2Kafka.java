package com.zhu.dts.core;

import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.zhu.dts.debezium.StringDebeziumDeserializationSchema;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.kafka.shaded.org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import com.zhu.dts.entity.ParameterEntity;
import com.zhu.dts.kafka.StringKafkaSerializationSchema;
import com.zhu.dts.util.SystemConfigConvertUtil;

import java.util.Properties;

/**
 * dts kafka 单分区写入
 *
 * @Author ZhuHaiBo
 * @Create 2021/11/30 1:01
 */
public class DbSync2Kafka {

    private static final String DEFAULT_TOPIC = "default_topic";

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 获取启动参数
        ParameterTool parameters = ParameterTool.fromArgs(args);
        ParameterEntity configEntity = SystemConfigConvertUtil.convert(parameters);

        // MysqlSource
        MySqlSource<String> mySqlSource = MySqlSource.<String>builder()
                .hostname(configEntity.getConfigHostname())
                .port(Integer.parseInt(configEntity.getConfigPort()))
                .databaseList(configEntity.getConfigDatabaseName())
                .tableList(configEntity.getConfigTableList())
                .username(configEntity.getConfigUsername())
                .password(configEntity.getConfigPassword())
                .deserializer(new StringDebeziumDeserializationSchema())
                .serverTimeZone("Asia/Shanghai")
                .startupOptions(StartupOptions.initial())
                .serverId(configEntity.getServiceIdRange())
                .build();

        // KafkaSink
        Properties kafkaProperties = new Properties();
        kafkaProperties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, configEntity.getConfigKafkaBootstrapServer());
        kafkaProperties.put(ProducerConfig.ACKS_CONFIG, "-1");
        kafkaProperties.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
        //properties.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "lz4");

        FlinkKafkaProducer<String> kafkaProducer = new FlinkKafkaProducer<>(DEFAULT_TOPIC,
                new StringKafkaSerializationSchema(Boolean.toString(configEntity.isMetadataFilter())), kafkaProperties, FlinkKafkaProducer.Semantic.AT_LEAST_ONCE);

        // enable checkpoint
        env.enableCheckpointing(configEntity.getConfigCheckpointInterval());

        // 下游sink设置并行度，默认为1
        env.fromSource(mySqlSource, WatermarkStrategy.noWatermarks(), "MySQL Source")
                .name("mysqlcdc-source")
                .uid("uid-mysqlcdc-source")
                .addSink(kafkaProducer).setParallelism(configEntity.getSinkParallelism())
                .name("kafka-sink").uid("kafka-sink");

        // flink job name
        env.execute(configEntity.getConfigTableList() + " DTS Start");
    }
}
