package dps;


import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import static org.apache.spark.sql.functions.*;

public class Q3_result {
    public static void main(String[] args) {
        SparkSession spark = SparkSession  
                            .builder()
                            .appName("Simple Application")
			                .config("spark.master","local[*]")
                            .getOrCreate();

        String intermediate_storage_path ="/home/ubuntu/hero/dps-q3/event-phase_1";
        
        //Read the intermediate data and summary the count
        
        Dataset<Row> activeUsers = spark.read().option("basePath", intermediate_storage_path + "/daily")
        .parquet(intermediate_storage_path + "/daily");
        
        Dataset<Row> result_of_daily = 
        activeUsers.groupBy(col("f_device_id")).agg(min("Date").alias("Date"))
        .selectExpr(
                "Date",
                "f_device_id",
                "'new' as AorN",
                "'daily' as Duration"
        ).union(
            activeUsers.selectExpr(
                "Date",
                "f_device_id",
                "'active' as AorN",
                "'daily' as Duration"
            )
        ).groupBy("Date","Duration","AorN")
        .agg(expr("COUNT('*') as count"));
        

        Dataset<Row> active_Weekly_User = spark.read().option("basePath", intermediate_storage_path + "/weekly")
        .parquet(intermediate_storage_path + "/weekly");

        Dataset<Row> result_of_weekly = 
        active_Weekly_User.groupBy(col("f_device_id")).agg(min("Date").alias("Date"))
        .selectExpr(
                "Date",
                "f_device_id",
                "'new' as AorN",
                "'weekly' as Duration"
        ).union(
            active_Weekly_User.selectExpr(
                "Date",
                "f_device_id",
                "'active' as AorN",
                "'weekly' as Duration"
            )
        ).groupBy("Date","Duration","AorN")
        .agg(expr("COUNT('*') as count"));
        

        result_of_daily
        .union(result_of_weekly)
        .orderBy("Date","AorN","Duration")
        //.coalesce(1)
        //.write().mode("overwrite").parquet("/home/ubuntu/hero/dps-q3/result/");
        .show(1000);
    }
}