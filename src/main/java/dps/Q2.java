package dps;

import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import static org.apache.spark.sql.functions.*;

public class Q2 {
    public static void main(String[] args) {
        SparkSession spark = SparkSession.builder().appName("Simple Application")
			                    .config("spark.master","local")
                                .getOrCreate();
        String data_source_for_partition_file = "s3a://pf-new-hire/partitioned_eventsmap/parquet/";
        String[] recived_days ={"2020-04-01","2020-04-02","2020-04-03","2020-04-04","2020-04-05","2020-04-06","2020-04-07"};


        Dataset<Row> events = 
        spark.read().option("basePath", data_source_for_partition_file)
        .parquet(data_source_for_partition_file + "/"+recived_days[0])
        .select(
				col("e_key"),
				col("f_timestamp_day"),
				col("e_segment_map"),
				expr(recived_days[0]).alias("recived_day")
		).filter(expr(
            "DATEDIFF(recived_day,f_timestamp_day) < 31 AND DATEDIFF(recived_day,f_timestamp_day) >= 0"
        ));


        Dataset<Row> count = 
        events
        .select(
            col("e_key"),
            col("f_timestamp_day"),
            explode(col("e_segment_map"))
        )
        .filter(
            col("key").rlike("sku.guid").or(
            col("key").rlike("item.guid"))
        ).groupBy("e_key","key")
        .agg(expr("COUNT('*') as count"))
        .select(
            col("e_key"),
            col("key").alias("feature"),
            col("count")
        );
    }
}