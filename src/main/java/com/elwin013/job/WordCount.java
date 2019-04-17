package com.elwin013.job;

import java.util.Arrays;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;

public class WordCount {
    private static final org.slf4j.Logger log = org.slf4j.LoggerFactory.getLogger(WordCount.class);
    private final JavaSparkContext sc;

    public WordCount(JavaSparkContext sc) {
        this.sc = sc;
    }

    public void run(String fileName, int resultLimit) {
        // Open file, create map, reduce
        final JavaPairRDD<String, Integer> wordCount = sc.textFile(fileName)
                .flatMap(line -> Arrays.asList(line.split(" ")).iterator())
                .mapToPair(word -> new Tuple2<>(word, 1))
                .reduceByKey((a, b) -> a + b);

        // Sort results and provide top resultLimit elements:
        final Map<String, Integer> wordCountMap = wordCount.collectAsMap()
                .entrySet().stream()
                .sorted(Map.Entry.comparingByValue(Comparator.reverseOrder()))
                .limit(resultLimit)
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue,
                        (e1, e2) -> e1, LinkedHashMap::new));

        // Print result.
        log.info("Words with the highest occurrence, in descending order:");
        wordCountMap.forEach((word, count) -> log.info("word: \"" + word + "\", counts: " + count));
    }
}
