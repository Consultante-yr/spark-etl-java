package org.projectspark.com.hive;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.projectspark.com.config.Config;

public class HiveReader extends org.projectspark.com.reader.ReaderClass {

	public HiveReader(Config appConfig) {
		super(appConfig);
	}

	@Override
	public JavaRDD<?> readRDD() {

		String query = (String) appConfig.getEntry(Config.QUERY);

		JavaRDD<?> data = spark.sql(query).toJavaRDD();

		return data;

	}

	@Override
	public Dataset<Row> readDataFrame() {

		String query = (String) appConfig.getEntry(Config.QUERY);

		Dataset<Row> data = spark.sql(query);
		return data;
	}

	@Override
	public <T> Dataset<T> readDataSet(Class<T> c) {

		String query = (String) appConfig.getEntry("hive.query");

		Dataset<T> data = spark.sql(query).as(Encoders.bean(c));

		return data;
	}

}
