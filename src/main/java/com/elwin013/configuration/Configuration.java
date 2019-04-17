package com.elwin013.configuration;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class Configuration {
    private static final org.slf4j.Logger log = org.slf4j.LoggerFactory.getLogger(Configuration.class);
    private AppProperties properties;

    public Configuration() {
        try (InputStream inputStream = this.getClass()
                .getClassLoader()
                .getResourceAsStream("application.properties")) {

            final Properties properties = new Properties();
            properties.load(inputStream);

            this.properties = new AppProperties();

            this.properties.setSparkExecutorCores(properties.getProperty("spark.executor.instances"));
            this.properties.setSparkExecutorInstances(properties.getProperty("spark.executor.cores"));

        } catch (IOException e) {
            log.error("Failed to load properties! ", e);
        }
    }

    public AppProperties getProperties() {
        return properties;
    }
}
