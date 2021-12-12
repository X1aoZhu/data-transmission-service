package com.zhu.dts.core;

import com.alibaba.fastjson.JSONObject;
import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.zhu.dts.debezium.JsonDebeziumDeserializationSchema;
import com.zhu.dts.kafka.JsonKafkaSerializationSchema;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.kafka.shaded.org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.flink.kafka.shaded.org.apache.kafka.common.config.TopicConfig;
import org.apache.flink.kafka.shaded.org.apache.kafka.common.config.internals.BrokerSecurityConfigs;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import com.zhu.dts.entity.ParameterEntity;
import com.zhu.dts.util.SystemConfigUtil;


import java.time.Duration;
import java.util.Properties;

/**
 * dts kafka 多分区写入
 *
 * @Author ZhuHaiBo
 * @Create 2021/12/9 2:31
 */
public class DbSync2KafkaV2 {

    private static final String DEFAULT_TOPIC = "default_topic";

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        ParameterTool parameterTool = ParameterTool.fromArgs(args);
        ParameterEntity configEntity = SystemConfigUtil.convert(parameterTool);

        MySqlSource<JSONObject> mySqlSource = MySqlSource.<JSONObject>builder()
                .databaseList(configEntity.getConfigDatabaseName())
                .tableList(configEntity.getConfigTableList())
                .hostname(configEntity.getConfigHostname())
                .username(configEntity.getConfigUsername())
                .password(configEntity.getConfigPassword())
                .port(Integer.parseInt(configEntity.getConfigPort()))
                .deserializer(new JsonDebeziumDeserializationSchema())
                .serverId(configEntity.getServiceIdRange())
                .connectTimeout(Duration.ofSeconds(3))
                .serverTimeZone("Asia/Shanghai")
                .startupOptions(StartupOptions.initial())
                .build();


        Properties kafkaProperties = new Properties();
        kafkaProperties.put(ProducerConfig.ACKS_CONFIG, "-1");
        kafkaProperties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, configEntity.getConfigKafkaBootstrapServer());
        kafkaProperties.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
        //properties.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "lz4");

        FlinkKafkaProducer<JSONObject> kafkaProducer = new FlinkKafkaProducer<>(DEFAULT_TOPIC, new JsonKafkaSerializationSchema(), kafkaProperties,
                FlinkKafkaProducer.Semantic.AT_LEAST_ONCE);

        // enable checkpoint
        env.enableCheckpointing(configEntity.getConfigCheckpointInterval());

        // sink kafka 根据pk进行多分区写入
        env.fromSource(mySqlSource, WatermarkStrategy.noWatermarks(), "mysql-source")
                .name("mysql-source").uid("mysql-source")
                .keyBy(((KeySelector<JSONObject, String>) jsonObject -> String.valueOf(jsonObject.get("pk"))))
                .addSink(kafkaProducer)
                .name("kafka-sink").uid("kafka-sink");

        // flink job name
        env.execute(configEntity.getConfigTableList() + " DTS V2 Start");
    }
}
