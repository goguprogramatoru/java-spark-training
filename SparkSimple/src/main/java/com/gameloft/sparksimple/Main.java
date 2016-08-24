package com.gameloft.sparksimple;

import generator.Generator;
import java.io.IOException;
import org.apache.commons.io.FileUtils;

import org.apache.log4j.Logger;
import org.apache.log4j.Level;

/**
 * Main - entry point
 * 
 * @author radu
 */
public class Main {
	
	private static final String CSV_FOLDER = "/home/gameloft/Documents/java/csv/";
	private static final String CSV_FILE = "sales.csv";
	private static final int NB_ROWS = 1 * 1000 * 1000;
	private static final String TABLE_NAME = "data";
	
	public static void main(String[] args) {
		// reduce spark logging (too much noise)
		Logger.getLogger("org").setLevel(Level.WARN);
		Logger.getLogger("akka").setLevel(Level.WARN);
		
		// make sure paths exist; create if not
		try {
			FileUtils.forceMkdir(FileUtils.getFile(CSV_FOLDER));
		} catch (IOException ex) {
			System.err.println(ex.getMessage());
		}
		
		// generate the file
		Generator.generateOneFile(NB_ROWS, CSV_FOLDER + CSV_FILE);
		
		Spark spark = new Spark();
		Csvs csvObject = new Csvs(spark);
		// load csv file as table using SaleModel class for schema
		csvObject.loadCsvsInferSchema(TABLE_NAME, CSV_FOLDER + CSV_FILE);
		// run simple query on table
		csvObject.doQuery("SELECT * FROM data");
	}
}