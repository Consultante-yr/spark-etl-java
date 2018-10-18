package org.projectspark.com.hbase;

import java.lang.reflect.InvocationTargetException;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.projectspark.com.config.Config;
import org.projectspark.com.util.SparkSessionClass;

public abstract class Reader {

	protected Config appConfig;

	protected SparkConf conf;

	protected SparkSession spark;

	public Reader(Config appConfig) {

		this.appConfig = appConfig;

		conf = new SparkConf().setAppName("HdfsSpark").setMaster("local[*]");

		spark = SparkSessionClass.getInstance();
	}

	public abstract JavaRDD<?> readRDD();

	public abstract Dataset<Row> readDataFrame()throws NoSuchMethodException, SecurityException, InstantiationException, IllegalAccessException, IllegalArgumentException, InvocationTargetException;

	public abstract<T> Dataset<T> readDataSet(Class<T> klass)throws NoSuchMethodException, SecurityException, InstantiationException, IllegalAccessException, IllegalArgumentException, InvocationTargetException;

}
