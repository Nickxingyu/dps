package dps;


import java.util.Arrays;
import java.util.HashSet;
import java.util.List;


import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import static org.apache.spark.sql.functions.*;


public class Q1
{
    public static void main( String[] args )
    {
        List<String> list_of_guid = Arrays.asList(
			"lipstick_item_guid", "blush_item_guid", "eyebrows_item_guid", "eyecolor_item_guid", "eyelashes_item_guid", 
			"eyeliner_item_guid", "foundation_item_guid", "wig_item_guid", "eyewear_pattern_guid", "headband_pattern_guid", 
            "necklace_pattern_guid", "earrings_pattern_guid", "fakeeyelashes_palette_guid", "haircolor_item_guid", "hairDye_item_guid");
            
        List<String> list_of_guid_list = Arrays.asList("eyeshadow_item_guid_list", "face_contour_item_guid_list");

		SparkSession spark = SparkSession
							.builder()
							.appName("Simple Application")
							.config("spark.master","local[*]")
                            .getOrCreate();
        String data_source_for_partition_file = "s3a://pf-new-hire/partitioned_eventsmap/parquet/";
		String intermediate_storage_path = "/home/ubuntu/hero/dps-q1/event-phase_1";

		String[] recived_days ={"2020-04-01","2020-04-02","2020-04-03","2020-04-04","2020-04-05","2020-04-06","2020-04-07"};

//Get data from difference directory and summary to intermediate_storage directory
		for(int i = 0; i < recived_days.length; i++){
			String recived_day = "'" + recived_days[i] + "'";
			//Select the col I want and drop the dirty data
			Dataset<Row> events = spark.read()
			.option("basePath",data_source_for_partition_file)
			.parquet(data_source_for_partition_file+"/"+recived_days[i]+"/")
			.select(
				col("e_key"),
				col("f_timestamp_day"),
				col("e_segment_map"),
				col("f_country"),
				col("f_os"),
				expr(recived_day).alias("recived_day")
			)
			.filter(expr(
				"DATEDIFF(recived_day,f_timestamp_day) < 31 AND DATEDIFF(recived_day,f_timestamp_day) >= 0"
			));

			//Explode the Maptype column 'e_segment_map'
			Dataset<Row> explode_events = events.select(
				col("e_key"),
				col("f_timestamp_day"),
				explode(col("e_segment_map")),
				col("f_country"),
				col("f_os")
            );

			//Get the guids from each event and count 
			Dataset<Row> guid_events = explode_events
			.filter(col("key").isin(list_of_guid.stream().toArray()))
			.groupBy(
				col("e_key"),
				col("f_timestamp_day").alias("Date"),
				col("f_country").alias("Country"),
				col("f_os").alias("OS"),
				col("key").alias("feature"))
			.agg(expr("COUNT('*')").alias("count"));


			//Create a new DataFrame from explode and drop the duplicate values of each guid 
			Dataset<Row> drop_duplicate_feature = spark.createDataFrame(
				explode_events.filter(col("key").isin(list_of_guid_list.stream().toArray()))
				.javaRDD().map(row->{
				String value = row.getString(3);
				String[] array_of_value = value.split("\\^");
				array_of_value = new HashSet<String>(Arrays.asList(array_of_value)).toArray(new String[0]);
				StringBuilder result = new StringBuilder();
				for(int index = 0; index < array_of_value.length; index++){
					result.append(array_of_value[index]);
					if(index == array_of_value.length - 1)
						break;
					result.append("^");
				}
				return RowFactory.create(row.get(0),row.get(1),row.get(2),result.toString(),row.get(4),row.get(5));
			}) , explode_events.schema());

			//Group and count
			Dataset<Row> guid_list_events = drop_duplicate_feature
			.withColumn("detial_value", explode(split(col("value"), "\\^")))
			.groupBy(
				col("e_key"),
				col("f_timestamp_day").alias("Date"),
				col("f_country").alias("Country"),
				col("f_os").alias("OS"),
				col("key").alias("feature"))
			.agg(expr("COUNT('*')").alias("count"));
			
			guid_events.write().mode("append").partitionBy("Date","e_key").parquet(intermediate_storage_path);
			guid_list_events.write().mode("append").partitionBy("Date","e_key").parquet(intermediate_storage_path);
		}	
    }
}
