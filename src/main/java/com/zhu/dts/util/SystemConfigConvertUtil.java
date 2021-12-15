package com.zhu.dts.util;

import com.zhu.dts.constant.ParameterConstants;
import com.zhu.dts.entity.ParameterEntity;
import org.apache.flink.api.java.utils.ParameterTool;

import java.util.Arrays;
import java.util.stream.Collectors;

/**
 * @Author ZhuHaiBo
 * @Create 2021/11/30 1:34
 */
public class SystemConfigConvertUtil {

    private static final String SPLIT = ",";

    private static final String ALL_TABLES_SUFFIX = ".*";

    public static ParameterEntity convert(ParameterTool parameters) {
        ParameterEntity parameterEntity = InitializeEntityUtil.getInstance();

        String databaseName = parameters.get(ParameterConstants.DATABASE_NAME);
        String tableListStr = parameters.get(ParameterConstants.TABLE_LIST);

        parameterEntity.setConfigHostname(parameters.get(ParameterConstants.HOSTNAME));
        parameterEntity.setConfigPort(parameters.get(ParameterConstants.PORT));
        parameterEntity.setConfigUsername(parameters.get(ParameterConstants.USERNAME));
        parameterEntity.setConfigPassword(parameters.get(ParameterConstants.PASSWORD));
        parameterEntity.setConfigDatabaseName(databaseName);
        parameterEntity.setMetadataFilter(Boolean.parseBoolean(parameters.get(ParameterConstants.METADATA_FILTER)));

        parameterEntity.setConfigKafkaBootstrapServer(parameters.get(ParameterConstants.KAFKA_BOOTSTRAP_SERVER));
        parameterEntity.setServiceIdRange(parameters.get(ParameterConstants.SERVICE_ID_RANGE));

        // TableList为空代表扫描当前库下所有表
        if (null == tableListStr || tableListStr.length() == 0) {
            parameterEntity.setConfigTableList(databaseName + ALL_TABLES_SUFFIX);
        } else {
            String tableStr = Arrays.stream(tableListStr.split(SPLIT))
                    .map(tableName -> databaseName + "." + tableName)
                    .collect(Collectors.joining(","));
            parameterEntity.setConfigTableList(tableStr);
        }

        // checkpoint interval
        String checkpoint = parameters.get(ParameterConstants.CHECKPOINT_INTERVAL);
        if (null == checkpoint || checkpoint.length() == 0) {
            parameterEntity.setConfigCheckpointInterval(3000L);
        } else {
            parameterEntity.setConfigCheckpointInterval(Long.parseLong(checkpoint));
        }

        // sink 并行度
        String sinkParallelismStr = parameters.get(ParameterConstants.SINK_PARALLELISM);
        try {
            if (null == sinkParallelismStr || Integer.parseInt(sinkParallelismStr) <= 0) {
                parameterEntity.setSinkParallelism(1);
            } else {
                parameterEntity.setSinkParallelism(Integer.parseInt(sinkParallelismStr));
            }
        } catch (NumberFormatException e) {
            parameterEntity.setSinkParallelism(1);
        }

        return parameterEntity;
    }
}
