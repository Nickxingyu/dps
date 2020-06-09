package dps;

import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import static org.apache.spark.sql.functions.*;

import java.time.LocalDate;
import java.time.format.DateTimeFormatter;

public class Q2 {
    public static void main(String[] args) {
        SparkSession spark = SparkSession.builder().appName("Simple Application")
			                    .config("spark.master","local[*]")
                                .getOrCreate();
        String data_source_for_partition_file = "s3a://pf-new-hire/partitioned_eventsmap/parquet/";

//Get data from difference directory and summary to intermediate_storage directory

        String intermediate_storage_path = "/data/tmp/dps-q2/event-phase_1";

        LocalDate start_date = LocalDate.parse("2020-04-01"); 
        LocalDate end_date = LocalDate.parse("2020-04-07"); 
        DateTimeFormatter formatter = DateTimeFormatter.ISO_LOCAL_DATE;
        for(
            LocalDate date = start_date;
            date.compareTo(end_date) <= 0; 
            date = date.plusDays(1)
        ){
            String received_date = date.format(formatter);

            //Select the col I want and drop the dirty data
            Dataset<Row> events = 
            spark.read().option("basePath", data_source_for_partition_file)
            .parquet(data_source_for_partition_file + "/"+received_date)
            .select(
                    col("e_key"),
                    col("f_timestamp_day"),
                    col("e_segment_map"),
                    expr("'"+received_date+"'").alias("received_date")
            ).filter(expr(
                "DATEDIFF(received_date,f_timestamp_day) < 31 AND DATEDIFF(received_date,f_timestamp_day) >= 0"
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
                col("key").rlike("sku_guid$|item_guid$")
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