package org.projectspark.com.hive;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.projectspark.com.config.Config;



public class HiveWrite extends org.projectspark.com.writer.WriteClass {

	public HiveWrite(Config appConfig) {
		super(appConfig);
		
	}


	public void WriteRDD(JavaRDD<?> rdd) {
	
		String path = (String) appConfig.getEntry(Config.OUTPUT_HIVE);
		
		rdd.saveAsTextFile(path);
		
		
	}

	
	public void WriteDataFrame(Dataset<Row> df) {
		String path = (String) appConfig.getEntry(Config.OUTPUT_HIVE);
		df.write().option("header", true).csv(path);
	}

	
	public <T> void  WriteDataSet(Dataset<?> ds) {
		
		String path = (String) appConfig.getEntry(Config.OUTPUT_HIVE);
		ds.write().option("header", true).csv(path);
	}

	
	
	
}
