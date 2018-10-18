package org.projectspark.com.writer;

import java.util.concurrent.ExecutionException;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.projectspark.com.config.Config;
import org.projectspark.com.util.SparkSessionClass;

public abstract class WriteClass {
	protected Config appConfig;

	protected SparkConf conf;

	protected SparkSession spark;

	public WriteClass(Config appConfig) {

		this.appConfig = appConfig;

		conf = new SparkConf().setAppName("HdfsSpark").setMaster("local[*]");

		spark = SparkSessionClass.getInstance();
	}

	public abstract void WriteRDD(JavaRDD<?> rdd) throws ExecutionException;

	public abstract void WriteDataFrame(Dataset<Row> df) throws ExecutionException;

	public abstract <T> void WriteDataSet(Dataset<?> ds) throws ExecutionException;
}
