package dps;

import org.apache.spark.sql.SparkSession;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import static org.apache.spark.sql.functions.*;

public class Q1Q2_result {
    public static void main(String[] args) {
		String start_date = "'"+args[0]+"'";
		String end_date = "'"+args[1]+"'";
        SparkSession spark = SparkSession
							.builder()
							.appName("Simple Application")
							.config("spark.master","local[*]")
                            .getOrCreate();
        String intermediate_storage_path = "/data/dps/q1q2/event-phase_1";
		//Read the intermediate data and summary the count
        Dataset<Row> intermediate_data = spark.read()
		.option("basePath", intermediate_storage_path)
		.parquet(intermediate_storage_path)
		.filter(expr(
			"DATEDIFF("+start_date+",received_date) < 31 AND DATEDIFF(received_date,"+end_date+") < 31 AND "+
			"DATEDIFF("+start_date+",date) < 31 AND DATEDIFF(date,"+end_date+") <= 0"
		))
		.groupBy(
			"date",
			"e_key",
			"country",
			"os",
			"feature",
			"value"
		).agg(sum("count").alias("count"));

		Dataset<Row> result =
		intermediate_data.filter(col("e_key").equalTo("YMK_Tryout"))
		.select(
				col("date"),
				col("country"),
				col("os"),
				col("feature"),
				col("value"),
				col("count").alias("tryout_count")
		)
		.join(
			intermediate_data.filter(col("e_key").equalTo("YMK_Capture"))
			.select(
				col("date").alias("c_date"),
				col("country").alias("c_country"),
				col("os").alias("c_os"),
				col("feature").alias("c_feature"),
				col("value").alias("c_value"),
				col("count").alias("capture_count")
			),
			col("date").equalTo(col("c_date")).and(
				col("country").equalTo(col("c_country"))
			).and(
				col("os").equalTo(col("c_os"))
			).and(
				col("feature").equalTo(col("c_feature"))
			).and(
                col("value").equalTo(col("c_value"))
            )
		).join(
			intermediate_data.filter(col("e_key").equalTo("YMK_Save"))
			.select(
				col("date").alias("s_date"),
				col("country").alias("s_country"),
				col("os").alias("s_os"),
				col("feature").alias("s_feature"),
				col("value").alias("s_value"),
				col("count").alias("save_count")
			), col("date").equalTo(col("s_date")).and(
				col("country").equalTo(col("s_country"))
			).and(
				col("os").equalTo(col("s_os"))
			).and(
				col("feature").equalTo(col("s_feature"))
			).and(
                col("value").equalTo(col("s_value"))
            )
		).selectExpr(
			"date","country","os","feature","value","tryout_count","capture_count","save_count"
		);

		result = result.withColumn("sku_or_item", col("feature"));

		result = spark.createDataFrame(
            result.javaRDD().map(row->{
				String feature = row.getString(3);
				String value = row.getString(4);
                String[] array_of_feature = feature.split("_");
                if(array_of_feature[1].equals("sku")){
                    return RowFactory.create(
						row.get(0),row.get(1),row.get(2),array_of_feature[0].toLowerCase(),
						value.toUpperCase(),row.get(5),row.get(6),row.get(7),"sku"
					);
                }else{
                    return RowFactory.create(
						row.get(0),row.get(1),row.get(2),array_of_feature[0].toLowerCase(),
						value.toUpperCase(),row.get(5),row.get(6),row.get(7),"item"
					);
                }
            }),
            result.schema());

		result =
		result
		.selectExpr(
				"date",
				"country",
				"os",
				"feature",
				"value",
				"sku_or_item",
				"tryout_count",
				"capture_count",
				"save_count"
		).union(
			result.groupBy("date","country","os","feature","sku_or_item")
			.agg(
				sum("tryout_count").alias("tryout_count"),
				sum("capture_count").alias("capture_count"),
				sum("save_count").alias("save_count")
			)
			.selectExpr(
				"date",
				"country",
				"os",
				"feature",
				"'All' as value",
				"sku_or_item",
				"tryout_count",
				"capture_count",
				"save_count"
			)
		).union(
			result.groupBy("date","country","os","value","sku_or_item")
			.agg(
				sum("tryout_count").alias("tryout_count"),
				sum("capture_count").alias("capture_count"),
				sum("save_count").alias("save_count")
			)
			.selectExpr(
				"date",
				"country",
				"os",
				"'All' as feature",
				"value",
				"sku_or_item",
				"tryout_count",
				"capture_count",
				"save_count"
			)
		).union(
			result.groupBy("date","country","feature","value","sku_or_item")
			.agg(
				sum("tryout_count").alias("tryout_count"),
				sum("capture_count").alias("capture_count"),
				sum("save_count").alias("save_count")
			)
			.selectExpr(
				"date",
				"country",
				"'All' as os",
				"feature",
				"value",
				"sku_or_item",
				"tryout_count",
				"capture_count",
				"save_count"
			)
		).union(
			result.groupBy("date","os","feature","value","sku_or_item")
			.agg(
				sum("tryout_count").alias("tryout_count"),
				sum("capture_count").alias("capture_count"),
				sum("save_count").alias("save_count")
			)
			.selectExpr(
				"date",
				"'All' as country",
				"os",
				"feature",
				"value",
				"sku_or_item",
				"tryout_count",
				"capture_count",
				"save_count"
			)
		).union(
			result.groupBy("date","country","os","sku_or_item")
			.agg(
				sum("tryout_count").alias("tryout_count"),
				sum("capture_count").alias("capture_count"),
				sum("save_count").alias("save_count")
			)
			.selectExpr(
				"date",
				"country",
				"os",
				"'All' as feature",
				"'All' as value",
				"sku_or_item",
				"tryout_count",
				"capture_count",
				"save_count"
			)
		).union(
			result.groupBy("date","country","feature","sku_or_item")
			.agg(
				sum("tryout_count").alias("tryout_count"),
				sum("capture_count").alias("capture_count"),
				sum("save_count").alias("save_count")
			)
			.selectExpr(
				"date",
				"country",
				"'All' as os",
				"feature",
				"'All' as value",
				"sku_or_item",
				"tryout_count",
				"capture_count",
				"save_count"
			)
		).union(
			result.groupBy("date","os","feature","sku_or_item")
			.agg(
				sum("tryout_count").alias("tryout_count"),
				sum("capture_count").alias("capture_count"),
				sum("save_count").alias("save_count")
			)
			.selectExpr(
				"date",
				"'All' as country",
				"os",
				"feature",
				"'All' as value",
				"sku_or_item",
				"tryout_count",
				"capture_count",
				"save_count"
			)
		).union(
			result.groupBy("date","country","value","sku_or_item")
			.agg(
				sum("tryout_count").alias("tryout_count"),
				sum("capture_count").alias("capture_count"),
				sum("save_count").alias("save_count")
			)
			.selectExpr(
				"date",
				"country",
				"'All' as os",
				"'All' as feature",
				"value",
				"sku_or_item",
				"tryout_count",
				"capture_count",
				"save_count"
			)
		).union(
			result.groupBy("date","os","value","sku_or_item")
			.agg(
				sum("tryout_count").alias("tryout_count"),
				sum("capture_count").alias("capture_count"),
				sum("save_count").alias("save_count")
			)
			.selectExpr(
				"date",
				"'All' as country",
				"os",
				"'All' as feature",
				"value",
				"sku_or_item",
				"tryout_count",
				"capture_count",
				"save_count"
			)
		).union(
			result.groupBy("date","feature","value","sku_or_item")
			.agg(
				sum("tryout_count").alias("tryout_count"),
				sum("capture_count").alias("capture_count"),
				sum("save_count").alias("save_count")
			)
			.selectExpr(
				"date",
				"'All' as country",
				"'All' as os",
				"feature",
				"value",
				"sku_or_item",
				"tryout_count",
				"capture_count",
				"save_count"
			)
		).union(
			result.groupBy("date","country","sku_or_item")
			.agg(
				sum("tryout_count").alias("tryout_count"),
				sum("capture_count").alias("capture_count"),
				sum("save_count").alias("save_count")
			)
			.selectExpr(
				"date",
				"country",
				"'All' as os",
				"'All' as feature",
				"'All' as value",
				"sku_or_item",
				"tryout_count",
				"capture_count",
				"save_count"
			)
		).union(
			result.groupBy("date","os","sku_or_item")
			.agg(
				sum("tryout_count").alias("tryout_count"),
				sum("capture_count").alias("capture_count"),
				sum("save_count").alias("save_count")
			)
			.selectExpr(
				"date",
				"'All' as country",
				"os",
				"'All' as feature",
				"'All' as value",
				"sku_or_item",
				"tryout_count",
				"capture_count",
				"save_count"
			)
		).union(
			result.groupBy("date","feature","sku_or_item")
			.agg(
				sum("tryout_count").alias("tryout_count"),
				sum("capture_count").alias("capture_count"),
				sum("save_count").alias("save_count")
			)
			.selectExpr(
				"date",
				"'All' as country",
				"'All' as os",
				"feature",
				"'All' as value",
				"sku_or_item",
				"tryout_count",
				"capture_count",
				"save_count"
			)
		).union(
			result.groupBy("date","value","sku_or_item")
			.agg(
				sum("tryout_count").alias("tryout_count"),
				sum("capture_count").alias("capture_count"),
				sum("save_count").alias("save_count")
			)
			.selectExpr(
				"date",
				"'All' as country",
				"'All' as os",
				"'All' as feature",
				"value",
				"sku_or_item",
				"tryout_count",
				"capture_count",
				"save_count"
			)
		).union(
			result.groupBy("date","sku_or_item")
			.agg(
				sum("tryout_count").alias("tryout_count"),
				sum("capture_count").alias("capture_count"),
				sum("save_count").alias("save_count")
			)
			.selectExpr(
				"date",
				"'All' as country",
				"'All' as os",
				"'All' as feature",
				"'All' as value",
				"sku_or_item",
				"tryout_count",
				"capture_count",
				"save_count"
			)
		).cache();

		result.groupBy("date","country","os","feature","value","sku_or_item")
		.agg(
			sum("tryout_count").alias("tryout_count"),
			sum("capture_count").alias("capture_count"),
			sum("save_count").alias("save_count")
		).write()
		.format("jdbc")
		.mode("append")
		.option("driver","com.mysql.jdbc.Driver")
		.option("url", "jdbc:mysql://localhost/app_events")
		.option("dbtable","event_count")
		.option("user", "root")
		.option("password","perfect")
		.save();
		/*
		result.show(100);
		*/
    }
}