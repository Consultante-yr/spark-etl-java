package org.projectspark.com.hbase;

import java.io.IOException;
import java.util.concurrent.ExecutionException;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.projectspark.com.config.Config;
import org.projectspark.com.util.SparkSessionClass;

public abstract class Writer {
	
		protected Config appConfig;

		protected SparkConf conf;

		protected SparkSession spark;

		public Writer(Config appConfig) {

			this.appConfig = appConfig;

			conf = new SparkConf().setAppName("HdfsSpark").setMaster("local[*]");

			spark = SparkSessionClass.getInstance();
		}

		public abstract void WriteRDD() throws ExecutionException, IOException;

		public abstract void WriteDataFrame() throws ExecutionException, IOException;

		public abstract <T> void WriteDataSet() throws ExecutionException , IOException;
	}


