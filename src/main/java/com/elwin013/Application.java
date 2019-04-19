package com.elwin013;

import com.elwin013.job.PIEstimator;
import com.elwin013.job.TextSearcher;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

import com.elwin013.configuration.AppProperties;
import com.elwin013.configuration.Configuration;
import com.elwin013.job.WordCount;
import org.apache.spark.sql.SparkSession;
import org.w3c.dom.Text;

public class Application {
    private static final org.slf4j.Logger log = org.slf4j.LoggerFactory.getLogger(Application.class);

    public static void main(String[] args) {
        // Initialize configuration
        final SparkConf sparkConf = new SparkConf();
        final AppProperties properties = new Configuration().getProperties();

        sparkConf.setMaster("local")
                .setAppName("AppName")
                .set("spark.executor.instances", properties.getSparkExecutorInstances())
                .set("spark.executor.cores", properties.getSparkExecutorCores());


        // Init Spark context
        final JavaSparkContext sc = new JavaSparkContext(sparkConf);

        // Run Word Count example
        WordCount wc = new WordCount(sc);
        wc.run("src/main/resources/exampleFile.txt", 10);

        PIEstimator pe = new PIEstimator(sc);
        pe.run(20);

        TextSearcher ts = new TextSearcher(sc);
        ts.run("src/main/resources/error.log");

        sc.close();
    }
}
