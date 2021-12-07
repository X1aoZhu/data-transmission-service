package com.zhu.dts.entity;



/**
 * @Author ZhuHaiBo
 * @Create 2021/11/30 1:41
 */
public class ParameterEntity {

    /**
     * mysql or pg
     */
    private String dataSourceType;

    private String configHostname;
    private String configPort;
    private String configUsername;
    private String configPassword;
    private String configDatabaseName;
    private String configTableList;

    private String configKafkaBootstrapServer;
    private String kafkaTopic;

    private String serviceIdRange;
    private Long configCheckpointInterval;

    private boolean metadataFilter;

    public ParameterEntity() {
    }

    public ParameterEntity(String dataSourceType, String configHostname, String configPort, String configUsername, String configPassword, String configDatabaseName, String configTableList, String configKafkaBootstrapServer, String kafkaTopic, String serviceIdRange, Long configCheckpointInterval, boolean metadataFilter) {
        this.dataSourceType = dataSourceType;
        this.configHostname = configHostname;
        this.configPort = configPort;
        this.configUsername = configUsername;
        this.configPassword = configPassword;
        this.configDatabaseName = configDatabaseName;
        this.configTableList = configTableList;
        this.configKafkaBootstrapServer = configKafkaBootstrapServer;
        this.kafkaTopic = kafkaTopic;
        this.serviceIdRange = serviceIdRange;
        this.configCheckpointInterval = configCheckpointInterval;
        this.metadataFilter = metadataFilter;
    }

    public String getDataSourceType() {
        return dataSourceType;
    }

    public void setDataSourceType(String dataSourceType) {
        this.dataSourceType = dataSourceType;
    }

    public String getConfigHostname() {
        return configHostname;
    }

    public void setConfigHostname(String configHostname) {
        this.configHostname = configHostname;
    }

    public String getConfigPort() {
        return configPort;
    }

    public void setConfigPort(String configPort) {
        this.configPort = configPort;
    }

    public String getConfigUsername() {
        return configUsername;
    }

    public void setConfigUsername(String configUsername) {
        this.configUsername = configUsername;
    }

    public String getConfigPassword() {
        return configPassword;
    }

    public void setConfigPassword(String configPassword) {
        this.configPassword = configPassword;
    }

    public String getConfigDatabaseName() {
        return configDatabaseName;
    }

    public void setConfigDatabaseName(String configDatabaseName) {
        this.configDatabaseName = configDatabaseName;
    }

    public String getConfigTableList() {
        return configTableList;
    }

    public void setConfigTableList(String configTableList) {
        this.configTableList = configTableList;
    }

    public String getConfigKafkaBootstrapServer() {
        return configKafkaBootstrapServer;
    }

    public void setConfigKafkaBootstrapServer(String configKafkaBootstrapServer) {
        this.configKafkaBootstrapServer = configKafkaBootstrapServer;
    }

    public String getConfigKafkaTopic() {
        return kafkaTopic;
    }

    public void setConfigKafkaTopic(String kafkaTopic) {
        this.kafkaTopic = kafkaTopic;
    }

    public String getServiceIdRange() {
        return serviceIdRange;
    }

    public void setServiceIdRange(String serviceIdRange) {
        this.serviceIdRange = serviceIdRange;
    }

    public String getKafkaTopic() {
        return kafkaTopic;
    }

    public void setKafkaTopic(String kafkaTopic) {
        this.kafkaTopic = kafkaTopic;
    }

    public Long getConfigCheckpointInterval() {
        return configCheckpointInterval;
    }

    public void setConfigCheckpointInterval(Long configCheckpointInterval) {
        this.configCheckpointInterval = configCheckpointInterval;
    }

    public boolean isMetadataFilter() {
        return metadataFilter;
    }

    public void setMetadataFilter(boolean metadataFilter) {
        this.metadataFilter = metadataFilter;
    }
}
