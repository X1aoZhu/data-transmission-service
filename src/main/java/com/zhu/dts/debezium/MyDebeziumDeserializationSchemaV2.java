package com.zhu.dts.debezium;

import com.ververica.cdc.connectors.shaded.org.apache.kafka.connect.source.SourceRecord;
import com.ververica.cdc.debezium.DebeziumDeserializationSchema;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.util.Collector;

/**
 * @Author ZhuHaiBo
 * @Create 2021/12/5 22:24
 */
//public class MyDebeziumDeserializationSchemaV2 implements DebeziumDeserializationSchema<String> {
//
//    @Override
//    public void deserialize(SourceRecord sourceRecord, Collector<String> collector) throws Exception {
//
//    }
//
//    @Override
//    public TypeInformation<String> getProducedType() {
//        return BasicTypeInfo.STRING_TYPE_INFO;
//    }
//}
