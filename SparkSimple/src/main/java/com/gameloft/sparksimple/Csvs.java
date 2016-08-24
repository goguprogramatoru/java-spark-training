package com.gameloft.sparksimple;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

/**
 *
 * @author radu
 */
public class Csvs {
	private final Spark spark;
	
	public Csvs(Spark spark) {
		this.spark = spark;
	}
	
	/**
	 * Run query on a previously registered table.
	 * 
	 * @param sqlQuery the query to run
	 */
	public void doQuery(String sqlQuery) {
		long startTime = System.nanoTime();
		
		// running the query
		// the query result will always be a DataFrame (Dataset<Row>)
		Dataset<Row> result = spark.getSparkSession().sql(sqlQuery);
		// showing some of the results
		result.show(50);
		
		long endTime = System.nanoTime();
		System.out.printf("Query took: %4.4f ms\n", ((endTime - startTime) / 1_000_000.0));
	}
	
	/**
	 * Create temp table from csv file to be used for sql
	 * The schema will be inferred from a model class (SaleModel)
	 * 
	 * @param tableName the name of the temp table to use
	 * @param filePath the full path to csv file
	 */
	public void loadCsvsInferSchema(String tableName, String filePath) {
		// create a rdd from the given csv file
		// the rdd will have String elements that are the individual lines
		// further processing is necessary to split the ines into pieces and map the model
		JavaRDD<String> rdd = spark.getSc().textFile(filePath);
		// or you can use the SparkSession instead:
		// JavaRDD<String> rdd = spark.getSparkSession().read().textFile(filePath).javaRDD();
		
		// use lambdas to transform each line element
		JavaRDD<SaleModel> saleModelRdd = rdd
				// remove all `"` characters, split the line by ','
				.map(line -> line.replaceAll("\"", "").split(","))
				// map each line to a SaleModel object to infer the schema for the resulting table
				.map(e -> new SaleModel(
						e[0],
						Integer.valueOf(e[1]),
						e[2],
						e[3]
					));
		// the temp table must be created from a DataFrame; we need to create the DataFrame from the RDD first
		Dataset<Row> df = spark.getSparkSession().createDataFrame(saleModelRdd, SaleModel.class);
		// register the temp table
		df.createOrReplaceTempView(tableName);
	}
}