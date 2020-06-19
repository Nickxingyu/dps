package dps;

import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import static org.apache.spark.sql.functions.*;

import java.time.LocalDate;
import java.time.format.DateTimeFormatter;

public class Q3 {
    
    public static void main(String[] args) {
        SparkSession spark = SparkSession.builder().appName("Simple Application")
			        .config("spark.master","local[*]")
                                .getOrCreate();
        String data_source_for_partition_file = "s3a://pf-new-hire/partitioned_eventsmap/parquet/";
        String intermediate_storage_path ="/data/tmp/dps-q3/event-phase_1";

        LocalDate start_date = LocalDate.parse(args[0]); 
        LocalDate end_date = LocalDate.parse(args[1]); 
        System.out.println(args[0]);
        System.out.println(args[1]);
        DateTimeFormatter formatter = DateTimeFormatter.ISO_LOCAL_DATE;
//Get data from difference directory and summary to intermediate_storage directory

        for(
                LocalDate date = start_date;
                date.compareTo(end_date) <= 0; 
                date = date.plusDays(1)
        ){
                String received_date = date.format(formatter);

                //Select the col I want and drop the dirty data
                Dataset<Row> events = spark.read().option("basePath", data_source_for_partition_file)
                        .parquet(data_source_for_partition_file+"/"+received_date + "/")
                        .selectExpr("f_timestamp_day","f_device_id")
                        .distinct();
                Dataset<Row> event_drop_dirty_data =
                events
                .select(
                        col("f_timestamp_day"),
                        col("f_device_id"),
			expr("'"+received_date+"'").alias("received_date")
		)
                .filter(expr(
                        "DATEDIFF(received_date,f_timestamp_day) < 31 AND DATEDIFF(received_date,f_timestamp_day) >= 0"
                ));

                //Start calculating daily and weekly active user
                Dataset<Row> activeUsers =
                event_drop_dirty_data
                .selectExpr("f_timestamp_day as date","f_device_id","received_date")
                .distinct();

                activeUsers
                .write().mode("append").partitionBy("received_date","date")
                .parquet(intermediate_storage_path);
        }
//End of forloop
    }
}