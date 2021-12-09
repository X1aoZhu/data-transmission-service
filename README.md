## flink data-transmission-service

### Doc
[flink-docs-release-1.13/docs/deployment](https://nightlies.apache.org/flink/flink-docs-release-1.13/docs/deployment/cli/)


### version
+  master branch: [flink 1.13.2，flink-cdc 2.1.0]
+  feature-1.13: [flink 1.13.3 flink-cdc 2.1.0]

### quick start

- local standalone
```shell script
# 多表同步，kafka单分区 kafka不指定key
bin/flink run -p 1 -d \
-c com.zhu.dts.core.DbSync2Kafka data-transmission-service-1.0-SNAPSHOT.jar \
--hostname 192.168.240.155 \
--port 3306 \
--username root \
--password root \
--database-name flink_cdc \
--table-list product,product_sale \
--bootstrap-server 192.168.240.155:9092 
[--service_id_range 5001][--checkpoint_interval 30000] 

# 多表同步，kafka多分区 指定kafka key
bin/flink run -p 1 -d \
-c com.zhu.dts.core.DbSync2KafkaV2 data-transmission-service-1.0-SNAPSHOT.jar \
--hostname 192.168.240.155 \
--port 3306 \
--username root \
--password root \
--database-name flink_cdc \
--table-list product,product_sale \
--bootstrap-server 192.168.240.155:9092 \
--partitions 4 \
--sink_parallelism 4 \ 
[--service_id_range 5001][--checkpoint_interval 30000] 
```
- yarn per-job
```shell script

```