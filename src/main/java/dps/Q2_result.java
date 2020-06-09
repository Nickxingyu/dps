package dps;

import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import static org.apache.spark.sql.functions.*;

public class Q2_result {
    public static void main(String[] args) {
        SparkSession spark = SparkSession
                                .builder()
                                .appName("Simple Application")
			                    .config("spark.master","local[*]")
                                .getOrCreate();

        String intermediate_storage_path = "/data/tmp/dps-q2/event-phase_1";

        //Read the intermediate data and summary the count
        Dataset<Row> result = 
        spark.read().option("basePath", intermediate_storage_path)
        .parquet(intermediate_storage_path)
        .groupBy("Date","e_key","feature").agg(sum("count").alias("count"))
        .orderBy("Date","e_key","feature");

        result
        .coalesce(1)
        .write().mode("overwrite").parquet("/data/tmp/dps-q2/result/");

        result.show(100);
    }
}