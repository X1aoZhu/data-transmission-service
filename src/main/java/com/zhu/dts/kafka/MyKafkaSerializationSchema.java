package com.zhu.dts.kafka;

import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.TypeReference;
import org.apache.flink.kafka.shaded.org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.flink.shaded.netty4.io.netty.util.internal.ObjectUtil;
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;

import javax.annotation.Nullable;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;

/**
 * @Author ZhuHaiBo
 * @Create 2021/11/30 22:08
 */
public class MyKafkaSerializationSchema implements KafkaSerializationSchema<String> {

    private static final String TOPIC_PREFIX = "ods_";

    private static final String TOPIC_SUFFIX = "_dts";

    public MyKafkaSerializationSchema() {
    }

    @Override
    public ProducerRecord<byte[], byte[]> serialize(String jsonStr, @Nullable Long aLong) {
        HashMap<String, Object> jsonObject = JSONObject.parseObject(jsonStr, new TypeReference<HashMap<String, Object>>() {
        });
        String table = JSONObject.parseObject(jsonObject.get("source").toString(), new TypeReference<HashMap<String, Object>>() {
        }).get("table").toString();
        String topic = TOPIC_PREFIX + table + TOPIC_SUFFIX;
        return new ProducerRecord<>(topic, jsonStr.getBytes(StandardCharsets.UTF_8));
    }
}
