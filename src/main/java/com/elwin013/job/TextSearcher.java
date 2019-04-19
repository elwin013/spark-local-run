package com.elwin013.job;

import java.util.Arrays;
import java.util.List;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.ForeachFunction;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import static org.apache.spark.sql.functions.col;


public class TextSearcher {
    private static final org.slf4j.Logger log = org.slf4j.LoggerFactory.getLogger(TextSearcher.class);
    private final JavaSparkContext sc;

    public TextSearcher(JavaSparkContext sc) {
        this.sc = sc;
    }

    public void run(String path) {
        // Creates a DataFrame having a single column named "line"
        SparkSession sqlContext = SparkSession.builder().getOrCreate();
        JavaRDD<String> textFile = sc.textFile(path);
        JavaRDD<Row> rowRDD = textFile.map(RowFactory::create);
        List<StructField> fields = Arrays.asList(
                DataTypes.createStructField("line", DataTypes.StringType, true));
        StructType schema = DataTypes.createStructType(fields);
        Dataset<Row> df = sqlContext.createDataFrame(rowRDD, schema);

        Dataset<Row> warnings = df.filter(col("line").like("%warn%"));

        log.info("Znaleziono " + warnings.count() + " ostrzezenia");
        warnings.foreach((ForeachFunction<Row>) row -> log.info(row.toString()));
    }
}
