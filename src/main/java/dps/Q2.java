package dps;

import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import static org.apache.spark.sql.functions.*;

public class Q2 {
    public static void main(String[] args) {
        SparkSession spark = SparkSession.builder().appName("Simple Application")
			                    .config("spark.master","local")
                                .getOrCreate();
        String data_source_for_partition_file = "s3a://pf-new-hire/partitioned_eventsmap/parquet/";
        String[] recived_days ={"2020-04-01","2020-04-02","2020-04-03","2020-04-04","2020-04-05","2020-04-06","2020-04-07"};

//Get data from difference directory and summary to intermediate_storage directory

        String intermediate_storage_path = "/home/ubuntu/hero/dps-q2/event-phase_1";
        for(int index = 0; index < recived_days.length; index++){
            String recived_day = "'" + recived_days[index] + "'";

            //Select the col I want and drop the dirty data
            Dataset<Row> events = 
            spark.read().option("basePath", data_source_for_partition_file)
            .parquet(data_source_for_partition_file + "/"+recived_days[index])
            .select(
                    col("e_key"),
                    col("f_timestamp_day"),
                    col("e_segment_map"),
                    expr(recived_day).alias("recived_day")
            ).filter(expr(
                "DATEDIFF(recived_day,f_timestamp_day) < 31 AND DATEDIFF(recived_day,f_timestamp_day) >= 0"
            ));

            //Filter the feature I want and count
            Dataset<Row> count = 
            events
            .select(
                col("e_key"),
                col("f_timestamp_day").alias("Date"),
                explode(col("e_segment_map"))
            )
            .filter(
                col("key").rlike("sku.guid").or(
                col("key").rlike("item.guid"))
            )
            .groupBy("Date","e_key","key").agg(expr("COUNT('*') as count"))
            .select(
                col("Date"),
                col("e_key"),
                col("key").alias("feature"),
                col("count")
            );
            count.write().mode("append").partitionBy("Date").parquet(intermediate_storage_path);
        }
    }
}