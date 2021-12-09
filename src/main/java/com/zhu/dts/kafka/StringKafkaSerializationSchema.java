package com.zhu.dts.kafka;

import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.TypeReference;
import com.alibaba.fastjson.serializer.SerializerFeature;
import org.apache.flink.kafka.shaded.org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;

import javax.annotation.Nullable;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;

/**
 * @Author ZhuHaiBo
 * @Create 2021/11/30 22:08
 */
public class StringKafkaSerializationSchema implements KafkaSerializationSchema<String> {

    private static final String TOPIC_PREFIX = "ods_";

    private static final String TOPIC_SUFFIX = "_dts";

    private static final String TRUE_FLAG = "true";

    private static final String FALSE_FLAG = "false";

    private final String metadataFilterFlag;

    public StringKafkaSerializationSchema(String metadataFilterFlag) {
        if (null == metadataFilterFlag || metadataFilterFlag.length() == 0) {
            this.metadataFilterFlag = FALSE_FLAG;
        } else {
            this.metadataFilterFlag = metadataFilterFlag;
        }
    }

    public StringKafkaSerializationSchema() {
        this.metadataFilterFlag = TRUE_FLAG;
    }

    @Override
    public ProducerRecord<byte[], byte[]> serialize(String jsonStr, @Nullable Long aLong) {
        HashMap<String, Object> jsonObject = JSONObject.parseObject(jsonStr, new TypeReference<HashMap<String, Object>>() {
        });

        String table = "";
        if (metadataFilterFlag.equals(TRUE_FLAG)) {
            // 移除source参数
            table = JSONObject.parseObject(jsonObject.remove("source").toString(), new TypeReference<HashMap<String, Object>>() {
            }).get("table").toString();
            // 移除transaction参数
            jsonObject.remove("transaction");
        } else {
            table = JSONObject.parseObject(jsonObject.get("source").toString(), new TypeReference<HashMap<String, Object>>() {
            }).get("table").toString();
        }

        String topic = TOPIC_PREFIX + table + TOPIC_SUFFIX;
        return new ProducerRecord<>(topic,
                JSONObject.toJSONString(jsonObject, SerializerFeature.WriteMapNullValue).getBytes(StandardCharsets.UTF_8));
    }
}
