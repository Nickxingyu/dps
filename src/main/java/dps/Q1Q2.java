package dps;

import java.time.LocalDate;
import java.util.Arrays;
import java.util.HashSet;
import java.util.ArrayList;
import java.util.List;
import java.time.format.DateTimeFormatter;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import static org.apache.spark.sql.functions.*;



public class Q1Q2
{
    public static void main( String[] args )
    {
		SparkSession spark = SparkSession
							.builder()
							.appName("Simple Application")
							.config("spark.master","local[*]")
							.getOrCreate();
		//spark.sparkContext().setLogLevel("ERROR");
        String data_source_for_partition_file = "s3a://pf-new-hire/partitioned_eventsmap/parquet/";
		String intermediate_storage_path = "/data/tmp/dps-q1q2/event-phase_1";

        List<String> list_of_guid = Arrays.asList(
			"eyeshadow_item_guid_list", "face_contour_item_guid_list","eyeshadow_sku_guid_list","eyeshadow_sku_guid_list",
			"lipstick_sku_guid", "blush_sku_guid", "eyebrows_sku_guid", "eyecolor_sku_guid", 
			"eyelashes_sku_guid", "eyeliner_sku_guid", "foundation_sku_guid", "wig_sku_guid", 
			"eyewear_sku_guid", "headband_sku_guid", "necklace_sku_guid", "earrings_sku_guid", 
			"fakeeyelashes_sku_guid", "haircolor_sku_guid", "hairDye_sku_guid",
			"lipstick_item_guid", "blush_item_guid", "eyebrows_item_guid", "eyecolor_item_guid", 
			"eyelashes_item_guid", "eyeliner_item_guid", "foundation_item_guid", "wig_item_guid",
			"eyewear_pattern_guid", "headband_pattern_guid", "necklace_pattern_guid", "earrings_pattern_guid", 
			"fakeeyelashes_palette_guid", "haircolor_item_guid", "hairDye_item_guid");
		
		LocalDate start_date = LocalDate.parse(args[0]); 
        LocalDate end_date = LocalDate.parse(args[1]); 
        DateTimeFormatter formatter = DateTimeFormatter.ISO_LOCAL_DATE;
		
//Get data from difference directory and summary to intermediate_storage directory
		for(
            LocalDate date = start_date;
            date.compareTo(end_date) <= 0; 
            date = date.plusDays(1)
        ){
			String received_date = date.format(formatter);
			//Select the col needed and drop the dirty data
			Dataset<Row> events = spark.read()
			.option("basePath",data_source_for_partition_file)
			.parquet(data_source_for_partition_file+"/"+received_date + "/")                                                               //+"a_complex_time=[0-9]*_[0-9]*0330")
			.select(
				col("e_key"),
				col("f_timestamp_day"),
				col("e_segment_map"),
				col("f_country"),
				col("f_os"),
				expr("'"+received_date+"'").alias("received_date")
			)
			.filter(expr(
				"DATEDIFF(received_date,f_timestamp_day) < 31 AND DATEDIFF(received_date,f_timestamp_day) >= 0"
			));

			//Explode the Maptype column 'e_segment_map'
			Dataset<Row> explode_events = events.select(
				col("e_key"),
				col("f_timestamp_day"),
				explode(col("e_segment_map")),
				col("f_country"),
				col("f_os"),
				col("received_date")
            );

			//Create a new DataFrame from explode and drop the duplicate values of each guid 
			
			Dataset<Row> drop_duplicate_feature = spark.createDataFrame(
				explode_events
				.filter(col("key").isin(list_of_guid.stream().toArray()))
				.javaRDD().flatMap(row->{
					List<Row> list = new ArrayList<Row>();
					String value = row.getString(3);
					String[] array_of_value = value.split("\\^");
					array_of_value = new HashSet<String>(Arrays.asList(array_of_value)).toArray(new String[0]);
					for(int index = 0; index < array_of_value.length; index++){
						list.add(
							RowFactory.create(row.get(0),row.get(1),row.get(2),array_of_value[index],row.get(4),row.get(5),row.get(6))
						);
					}
					return list.iterator();
				}), 
				explode_events.schema()
			);
			//Group and count
			Dataset<Row> guid_events = drop_duplicate_feature
			.groupBy(
				col("e_key"),
				col("f_timestamp_day").alias("date"),
				col("f_country").alias("country"),
				col("f_os").alias("os"),
				col("key").alias("feature"),
				col("value"),
				col("received_date")
			)
			.agg(expr("COUNT('*')").alias("count"));
	
			guid_events
			.write().mode("append").partitionBy("received_date","date","e_key").parquet(intermediate_storage_path);
		}	
    }
}