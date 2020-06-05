package dps;

import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import static org.apache.spark.sql.functions.*;

public class Q3 {
    
    public static void main(String[] args) {
        SparkSession spark = SparkSession.builder().appName("Simple Application")
			        .config("spark.master","local")
                                .getOrCreate();
        String data_source_for_partition_file = "s3a://pf-new-hire/partitioned_eventsmap/parquet/";
        String[] recived_days ={"2020-04-01","2020-04-02","2020-04-03","2020-04-04","2020-04-05","2020-04-06","2020-04-07"};

//Get data from difference directory and summary to intermediate_storage directory

        String intermediate_storage_path ="/home/ubuntu/hero/dps-q3/event-phase_1";

        for(int index = 0; index < recived_days.length; index++){
                String recived_day = "'" + recived_days[index] + "'";

                //Select the col I want and drop the dirty data
                Dataset<Row> events = spark.read().option("basePath", data_source_for_partition_file)
                        .parquet(data_source_for_partition_file+"/"+recived_days[index])
                        .selectExpr("f_timestamp_day","f_device_id")
                        .distinct();
                Dataset<Row> event_drop_dirty_data =
                events
                .select(
                        col("f_timestamp_day"),
                        col("f_device_id"),
			expr(recived_day).alias("recived_day")
		)
                .filter(expr(
                        "DATEDIFF(recived_day,f_timestamp_day) < 31 AND DATEDIFF(recived_day,f_timestamp_day) >= 0"
                )).cache();

                //Start calculating daily user
                Dataset<Row> activeUsers =
                event_drop_dirty_data.distinct()
                .selectExpr("f_timestamp_day as Date","f_device_id");

                //End of active daily user
                activeUsers.groupBy("Date").agg(expr("COUNT('*') as count"))
                .selectExpr(
                        "Date",
                        "'Daily' as Duration",
                        "'Active' as AorN",
                        "count"
                ).write().mode("append").partitionBy("Date").parquet(intermediate_storage_path);

                //End of new daily user
                Dataset<Row> newUsers = 
                activeUsers.groupBy(col("f_device_id")).agg(min("Date").alias("Date"))
                .groupBy("Date").agg(expr("COUNT('*') as count"))
                .selectExpr(
                        "Date",
                        "'Daily' as Duration",
                        "'New' as AorN",
                        "count"
                );
                newUsers.write().mode("append").partitionBy("Date").parquet(intermediate_storage_path);

                //Start calculating weekly user
                Dataset<Row> active_Weekly_User = 
                event_drop_dirty_data.withColumn("last_monday", date_sub(next_day(col("f_timestamp_day"), "monday"), 7))
                .selectExpr("last_monday as Date","f_device_id")
                .dropDuplicates("Date","f_device_id");
                
                //End of active weekly user
                active_Weekly_User
                .groupBy("Date").agg(expr("COUNT('*') as count"))
                .selectExpr(
                        "Date",
                        "'Weekly' as Duration",
                        "'Active' as AorN",
                        "count"
                ).write().mode("append").partitionBy("Date").parquet(intermediate_storage_path);//.show();

                //End of new weekly user
                Dataset<Row> new_Weekly_User =
                active_Weekly_User
                .groupBy(col("f_device_id")).agg(min("Date").alias("Date"))
                .groupBy("Date").agg(expr("COUNT('*') as count"))
                .selectExpr(
                        "Date",
                        "'Weekly' as Duration",
                        "'New' as AorN ",
                        "count"
                );
                new_Weekly_User.write().mode("append").partitionBy("Date").parquet(intermediate_storage_path);
        }
//End of forloop
    }
}