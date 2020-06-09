package dps;


import java.time.LocalDate;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;

import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import static org.apache.spark.sql.functions.*;


public class Q1
{
    public static void main( String[] args )
    {
		SparkSession spark = SparkSession
							.builder()
							.appName("Simple Application")
							.config("spark.master","local[*]")
                            .getOrCreate();
        String data_source_for_partition_file = "s3a://pf-new-hire/partitioned_eventsmap/parquet/";
		String intermediate_storage_path = "/data/tmp/dps-q1/event-phase_1";

		LocalDate start_date = LocalDate.parse("2020-04-01"); 
        LocalDate end_date = LocalDate.parse("2020-04-07"); 
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
			.parquet(data_source_for_partition_file+"/"+received_date)
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
				col("f_os")
            );

			//Get the guids from each event and count 
			Dataset<Row> guid_events = explode_events
			.filter(col("key").rlike("item_guid$|pattern_guid$"))
			.filter(col("value").isNotNull())
			.groupBy(
				col("e_key"),
				col("f_timestamp_day").alias("Date"),
				col("f_country").alias("Country"),
				col("f_os").alias("OS"),
				col("key").alias("feature"))
			.agg(expr("COUNT('*')").alias("count"));


			//Create a new DataFrame from explode and drop the duplicate values of each guid 
			Dataset<Row> drop_duplicate_feature = spark.createDataFrame(
				explode_events.filter(col("key").endsWith("item_guid_list"))
				.javaRDD().map(row->{
				String value = row.getString(3);
				String[] array_of_value = value.split("\\^");//**********
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
			.filter(col("detial_value").isNotNull())
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
