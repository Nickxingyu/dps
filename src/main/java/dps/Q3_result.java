package dps;

import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import static org.apache.spark.sql.functions.*;

public class Q3_result {
    public static void main(String[] args) {
        String start_date = "'"+args[0]+"'";
		String end_date = "'"+args[1]+"'";
        SparkSession spark = SparkSession  
                            .builder()
                            .appName("Simple Application")
			                .config("spark.master","local[*]")
                            .getOrCreate();

        String intermediate_storage_path ="/data/dps/q3/event-phase_1";
        //Read the intermediate data and summary the count

        Dataset<Row> activeUsers = spark.read().option("basePath", intermediate_storage_path)
        .parquet(intermediate_storage_path)
        .filter(expr(
			"DATEDIFF("+start_date+",received_date) < 31 AND DATEDIFF(received_date,"+end_date+") < 31 AND "+
			"DATEDIFF("+start_date+",date) < 31 AND DATEDIFF(date,"+end_date+") <= 0"
		))
        .distinct()
        .selectExpr("date","f_device_id");
        
        activeUsers
        .groupBy("f_device_id").agg(min("date").alias("date"))
        .groupBy("date").agg(expr("COUNT(*) as new_users_count"));

        Dataset<Row> active_weekly_Users =
        activeUsers
        .withColumn("last_monday", date_sub(next_day(col("date"), "monday"), 7))
        .selectExpr("last_monday as date","f_device_id")
        .distinct();

        active_weekly_Users
        .groupBy("f_device_id").agg(min("date").alias("date"))
        .groupBy("date").agg(expr("COUNT(*) as new_users_count"));

        Dataset<Row> result =
        activeUsers.groupBy("date").agg(expr("COUNT(*) as active_users_count"))
        .join(
            activeUsers
            .groupBy("f_device_id").agg(min("date").alias("date"))
            .groupBy("date").agg(expr("COUNT(*) as new_users_count"))
            .selectExpr("date as n_date","new_users_count"),
            col("date").equalTo(col("n_date"))
        ).selectExpr("date","'daily' as duration","active_users_count","new_users_count")
        .union(
            active_weekly_Users.groupBy("date").agg(expr("COUNT(*) as active_users_count"))
            .join(
                active_weekly_Users
                .groupBy("f_device_id").agg(min("date").alias("date"))
                .groupBy("date").agg(expr("COUNT(*) as new_users_count"))
                .selectExpr("date as n_date","new_users_count"),
                col("date").equalTo(col("n_date"))
            ).selectExpr("date","'weekly' as duration","active_users_count","new_users_count")
        ).orderBy(col("date"),col("duration")).cache();
        //.show(100);
        result.show(100);
        
        result.write()
            .format("jdbc")
            .mode("append")
            .option("driver","com.mysql.jdbc.Driver")
            .option("url", "jdbc:mysql://localhost/app_events")
            .option("dbtable","active_users")
            .option("user", "root")
            .option("password","perfect")
            .save();
         
    }
}