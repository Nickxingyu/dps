package dps;

import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import static org.apache.spark.sql.functions.*;

public class Q1_result {
    public static void main(String[] args) {
        SparkSession spark = SparkSession
							.builder()
							.appName("Simple Application")
							.config("spark.master","local[*]")
                            .getOrCreate();
        String intermediate_storage_path = "/home/ubuntu/hero/dps-q1/event-phase_1";

		//Read the intermediate data and summary the count
        Dataset<Row> result = spark.read()
		.option("basePath", intermediate_storage_path)
		.parquet(intermediate_storage_path)
		.groupBy(
			"Date",
			"e_key",
			"Country",
			"OS",
			"feature"
		).agg(sum("count").alias("count"))
		.orderBy("Date","e_key","Country","OS","feature");

        result
		.coalesce(1)
		.write().mode("overwrite").parquet("/home/ubuntu/hero/dps-q1/result");

		result.show(100);
		
    }
}