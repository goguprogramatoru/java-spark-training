package com.gameloft.sparksimple;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.SparkSession;

/**
 * Wrapper class for spark object.
 * We should have only one spark context and session.
 * 
 * @author radu
 */

public class Spark {
	
	private final SparkConf conf;
	private final JavaSparkContext sc;
	private final SparkSession sparkSession;

	public SparkConf getConf() {
		return conf;
	}

	public JavaSparkContext getSc() {
		return sc;
	}
	
	public SparkSession getSparkSession() {
		return sparkSession;
	}

	public Spark() {
		// create a spark config object
		this.conf = new SparkConf().setAppName("Spark Word Count").setMaster("local[*]");
		// create a spark context with the previously defined config
		this.sc = new JavaSparkContext(conf);
		// create the spark session
		this.sparkSession = SparkSession.builder().config(conf).getOrCreate();
		
	}
	
	public void disconnect() {
		if (sc != null) {
			sc.stop();
		}
	}

}