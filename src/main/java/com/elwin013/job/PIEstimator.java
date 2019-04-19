package com.elwin013.job;

import java.util.List;
import java.util.ArrayList;
import org.apache.spark.api.java.JavaSparkContext;

public class PIEstimator {
    private static final org.slf4j.Logger log = org.slf4j.LoggerFactory.getLogger(PIEstimator.class);
    private final JavaSparkContext sc;

    public PIEstimator(JavaSparkContext sc) {
        this.sc = sc;
    }

    public void run(int NUM_SAMPLES) {
        List<Integer> l = new ArrayList<>(NUM_SAMPLES);
        for (int i = 0; i < NUM_SAMPLES; i++) {
            l.add(i);
        }

        long count = sc.parallelize(l).filter(i -> {
            double x = Math.random();
            double y = Math.random();
            return x*x + y*y < 1;
        }).count();

        double res = 4.0 * count / NUM_SAMPLES;

        log.info("Wyliczone pi razy drzwi: " + res);
    }
}
