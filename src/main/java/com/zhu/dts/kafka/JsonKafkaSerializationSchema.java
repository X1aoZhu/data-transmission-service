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
public class JsonKafkaSerializationSchema implements KafkaSerializationSchema<JSONObject> {

    private static final String TOPIC_PREFIX = "ods_";

    private static final String TOPIC_SUFFIX = "_dts";

    private static final String TRUE_FLAG = "true";

    private static final String FALSE_FLAG = "false";

    private final String metadataFilterFlag;

    public JsonKafkaSerializationSchema(String metadataFilterFlag) {
        if (null == metadataFilterFlag || metadataFilterFlag.length() == 0) {
            this.metadataFilterFlag = FALSE_FLAG;
        } else {
            this.metadataFilterFlag = metadataFilterFlag;
        }
    }

    public JsonKafkaSerializationSchema() {
        this.metadataFilterFlag = TRUE_FLAG;
    }

    @Override
    public ProducerRecord<byte[], byte[]> serialize(JSONObject jsonObject, @Nullable Long aLong) {
        String table = JSONObject.parseObject(jsonObject.get("source").toString(),
                new TypeReference<HashMap<String, Object>>() {
                })
                .get("table").toString();
        String topic = TOPIC_PREFIX + table + TOPIC_SUFFIX;
        String key = jsonObject.remove("pk").toString();
        return new ProducerRecord<>(topic,
                key.getBytes(StandardCharsets.UTF_8), jsonObject.toString(SerializerFeature.WriteMapNullValue).getBytes(StandardCharsets.UTF_8));
    }
}
