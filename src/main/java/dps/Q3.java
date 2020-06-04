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

// Remember adding forloop to summary all data finally
        Dataset<Row> events = spark.read().option("basePath", data_source_for_partition_file)
                    .parquet(data_source_for_partition_file+recived_days[0])
                    .selectExpr("f_timestamps_day","f_device_id")
                    .distinct();
        events.write().partitionBy("f_timestamps_day").parquet("/home/ubuntu/hero/dsp-q2/event-phase_1/");
        

        Dataset<Row> event_phase_1 = spark.read().option("basePath", "/home/ubuntu/hero/dsp-q2/event-phase_1")
                                        .parquet("/home/ubuntu/hero/dsp-q2/event-phase_1");
//End of forloop


//Dailly active users and new users
        Dataset<Row> activeUsers = event_phase_1.distinct()
        .selectExpr("f_timestamp_day as Date","f_device_id");
        Dataset<Row> newUsers = activeUsers.groupBy(col("f_device_id")).agg(min("Date"));

        activeUsers.groupBy("Date").agg(expr("COUNT('*') as count"));
        newUsers.groupBy("Date").agg(expr("COUNT('*') as count"));
//Weekly active users and new users

        //event_phase_1 with column "last monday"
        Dataset<Row> active_Weekly_User = 
        event_phase_1.withColumn("last_monday", date_sub(next_day(col("f_timestamp_day"), "monday"), 7))
        .selectExpr("last_monday as Date","f_device_id")
        .dropDuplicates("f_device_id","last_monday");

        Dataset<Row> new_Weekly_User =
        active_Weekly_User.groupBy(col("f_device_id")).agg(min(col("last_monday")));
        //and useing the dailly method on "last monday" and "f_device_id" 

        active_Weekly_User.groupBy("Date").agg(expr("COUNT('*') as count"));
        new_Weekly_User.groupBy("Date").agg(expr("COUNT('*') as count"));

// count 
    }
}