package com.zhu.dts.debezium;


import com.ververica.cdc.connectors.shaded.org.apache.kafka.connect.data.Schema;
import com.ververica.cdc.connectors.shaded.org.apache.kafka.connect.json.JsonConverter;
import com.ververica.cdc.connectors.shaded.org.apache.kafka.connect.source.SourceRecord;
import com.ververica.cdc.connectors.shaded.org.apache.kafka.connect.storage.ConverterType;
import com.ververica.cdc.debezium.DebeziumDeserializationSchema;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.util.Collector;

import java.util.HashMap;

/**
 * @Author ZhuHaiBo
 * @Create 2021/12/1 1:59
 */
public class MyDebeziumDeserializationSchema extends JsonConverter implements DebeziumDeserializationSchema<String> {

    private transient JsonConverter jsonConverter;

    @Override
    public void deserialize(SourceRecord record, Collector<String> collector) throws Exception {
        if (this.jsonConverter == null) {
            this.jsonConverter = new JsonConverter();
            HashMap<String, Object> configs = new HashMap<>(2);
            configs.put("converter.type", ConverterType.VALUE.getName());
            configs.put("schemas.enable", "false");
            this.jsonConverter.configure(configs);
        }

        byte[] bytes =
                jsonConverter.fromConnectData(record.topic(), record.valueSchema(), record.value());
        //HashMap<String, Object> parseObject = JSONObject.parseObject(new String(bytes), new TypeReference<HashMap<String, Object>>() {
        //});
        //HashMap<String, Object> source = (HashMap<String, Object>) parseObject.get("source");
        //source.remove("version");
        //collector.collect(String.valueOf(parseObject));
        collector.collect(new String(bytes));
    }

    @Override
    public TypeInformation<String> getProducedType() {
        return BasicTypeInfo.STRING_TYPE_INFO;
    }

    @Override
    public byte[] fromConnectData(String topic, Schema schema, Object value) {
        return super.fromConnectData(topic, schema, value);
    }

}
