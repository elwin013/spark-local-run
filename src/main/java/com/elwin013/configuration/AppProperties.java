package com.elwin013.configuration;

public class AppProperties {
    private String sparkExecutorInstances;
    private String sparkExecutorCores;

    public String getSparkExecutorInstances() {
        return sparkExecutorInstances;
    }

    void setSparkExecutorInstances(String sparkExecutorInstances) {
        this.sparkExecutorInstances = sparkExecutorInstances;
    }

    public String getSparkExecutorCores() {
        return sparkExecutorCores;
    }

    void setSparkExecutorCores(String sparkExecutorCores) {
        this.sparkExecutorCores = sparkExecutorCores;
    }
}
