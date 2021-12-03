package com.zhu.dts.util;

import com.zhu.dts.constant.ParameterConstants;
import com.zhu.dts.entity.ParameterEntity;
import org.apache.flink.api.java.utils.ParameterTool;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.stream.Collectors;

/**
 * @Author ZhuHaiBo
 * @Create 2021/11/30 1:34
 */
public class SystemConfigUtil {

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

        String checkpoint = parameters.get(ParameterConstants.CHECKPOINT_INTERVAL);
        if (null == checkpoint || checkpoint.length() == 0) {
            parameterEntity.setConfigCheckpointInterval(3000L);
        } else {
            parameterEntity.setConfigCheckpointInterval(Long.parseLong(checkpoint));
        }

        return parameterEntity;
    }
}
