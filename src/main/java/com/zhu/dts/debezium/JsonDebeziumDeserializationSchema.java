package com.zhu.dts.debezium;

import com.alibaba.fastjson.JSONObject;
import com.ververica.cdc.connectors.shaded.org.apache.kafka.connect.data.Field;
import com.ververica.cdc.connectors.shaded.org.apache.kafka.connect.data.Struct;
import com.ververica.cdc.connectors.shaded.org.apache.kafka.connect.source.SourceRecord;
import com.ververica.cdc.debezium.DebeziumDeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.util.Collector;
import io.debezium.data.Envelope;


import java.util.List;

/**
 * @Author ZhuHaiBo
 * @Create 2021/12/5 22:24
 */
public class JsonDebeziumDeserializationSchema implements DebeziumDeserializationSchema<JSONObject> {

    @Override
    public void deserialize(SourceRecord record, Collector<JSONObject> collector) throws Exception {
        JSONObject result = new JSONObject();

        // 获取操作
        Envelope.Operation op = Envelope.operationFor(record);

        // pk
        Struct keyStruck = (Struct) record.key();
        StringBuilder pk = new StringBuilder();
        List<Field> fields = keyStruck.schema().fields();
        for (Field field : fields) {
            pk.append(keyStruck.get(field));
        }

        // 获取数据本身
        Struct values = (Struct) record.value();

        // before
        if (op != Envelope.Operation.CREATE && op != Envelope.Operation.READ) {
            Struct before = values.getStruct(Envelope.FieldName.BEFORE);
            List<Field> beforeSchemaField = before.schema().fields();
            JSONObject beforeJson = new JSONObject();

            beforeSchemaField.forEach(field -> beforeJson.put(field.name(), before.get(field)));
            result.put(Envelope.FieldName.BEFORE, beforeJson);
        } else {
            result.put(Envelope.FieldName.BEFORE, null);
        }

        // after
        JSONObject afterJson = new JSONObject();
        Struct after = values.getStruct(Envelope.FieldName.AFTER);
        List<Field> afterFields = after.schema().fields();
        afterFields.forEach(field -> afterJson.put(field.name(), after.get(field)));

        // source
        Struct source = values.getStruct(Envelope.FieldName.SOURCE);
        List<Field> sourceFields = source.schema().fields();
        JSONObject sourceJson = new JSONObject();
        sourceFields.forEach(field -> sourceJson.put(field.name(), source.get(field)));

        result.put("pk", pk.toString());
        result.put(Envelope.FieldName.OPERATION, op.code());
        result.put(Envelope.FieldName.AFTER, afterJson);
        result.put(Envelope.FieldName.SOURCE, sourceJson);

        // ts_ms
        result.put(Envelope.FieldName.TIMESTAMP, values.get(Envelope.FieldName.TIMESTAMP));

        //todo transaction
        //Field transactionField = record.valueSchema().field(Envelope.FieldName.TRANSACTION);
        //result.put(Envelope.FieldName.TRANSACTION, "");

        collector.collect(result);
    }

    @Override
    public TypeInformation<JSONObject> getProducedType() {
        return TypeInformation.of(JSONObject.class);
    }
}
