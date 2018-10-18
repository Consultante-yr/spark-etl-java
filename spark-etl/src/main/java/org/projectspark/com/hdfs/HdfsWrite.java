package org.projectspark.com.hdfs;

import java.io.File;
import java.util.concurrent.ExecutionException;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.rdd.RDD;
import org.apache.spark.sql.DataFrameWriter;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.execution.datasources.text.TextFileFormat;
import org.projectspark.com.config.Config;

public class HdfsWrite extends org.projectspark.com.writer.WriteClass {

	public HdfsWrite(Config appConfig) {
		super(appConfig);
		
	}


	public  void  WriteRDD(JavaRDD<?> rdd) {
		
		String file_out = (String) appConfig.getEntry("output.path");
		//JavaRDD<?> data = spark.sparkContext().textFile(source, 1).toJavaRDD();
         rdd.saveAsTextFile(file_out);
    
		
		
	}


	public void  WriteDataFrame(Dataset<Row> df) {
		
		String file_out = (String) appConfig.getEntry("output.path");
		
		df.write().option("header", true).csv(file_out);
		
	 
	
	
	
	}





	public <T>  void  WriteDataSet(Dataset<?> ds) {
		String file_out = (String) appConfig.getEntry("output.path");
		ds.write().option("header", true).csv(file_out);
	}


	


	



}
