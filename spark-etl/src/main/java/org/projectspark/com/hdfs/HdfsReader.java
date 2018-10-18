package org.projectspark.com.hdfs;


import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.rdd.RDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.projectspark.com.config.Config;
import org.projectspark.com.reader.ReaderClass;

public class HdfsReader  extends ReaderClass {
	
	
	public HdfsReader(Config appConfig) {
		super(appConfig);
	}





	public  JavaRDD<?> readRDD() {
		String source = (String) appConfig.getEntry("input.path");
		RDD<?> data = spark.sparkContext().textFile(source, 1);

		return data.toJavaRDD();
	}
	

	


	public Dataset<Row> readDataFrame() {
		
		String source = (String) appConfig.getEntry("input.path");
		//String format = (String) appConfig.getEntry(Config.FILE_FORMAT);
		
		RDD<String> data = spark.sparkContext().textFile(source, 1);
		
		return spark.read().format("csv").option("header", true).load(source);
	
		
	}


	public  <T> Dataset<T> readDataSet(Class<T> klass) {

		String source = (String) appConfig.getEntry("input.path");
		//String format = (String) appConfig.getEntry(Config.FILE_FORMAT);
		
		RDD<String> data = spark.sparkContext().textFile(source, 1);
		Dataset<?> dd= spark.read().format("csv").option("header", true).load(source);
		return (Dataset<T>) dd;
		
	}










	


}
