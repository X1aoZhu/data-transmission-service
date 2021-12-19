package com.zhu.dts.constant;

import java.util.List;

/**
 *
 * @Author ZhuHaiBo
 * @Create 2021/11/30 1:23
 */
public class ParameterConstants {

    public final static String HOSTNAME = "hostname";
    public final static String PORT = "port";
    public final static String USERNAME = "username";
    public final static String PASSWORD = "password";
    public final static String DATABASE_NAME = "database-name";

    /**
     * pg param
     */
    public final static String PG_SCHEMA_NAME = "schema-name";

    /**
     * example: table1,table2,...,tableN
     */
    public final static String TABLE_LIST = "table-list";

    /**
     * example: 192.168.240.155:9092,192.168.240.155:9093,192.168.240.155:9094
     */
    public final static String KAFKA_BOOTSTRAP_SERVER = "bootstrap-server";

    /**
     * example: 5001-5004
     */
    public final static String SERVICE_ID_RANGE = "service-id";

    public final static String CHECKPOINT_INTERVAL = "checkpoint-interval";

    /**
     * default: false
     */
    public final static String METADATA_FILTER = "metadata-flag";

}
