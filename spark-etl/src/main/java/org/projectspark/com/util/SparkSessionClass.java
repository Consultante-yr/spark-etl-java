package org.projectspark.com.util;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.SparkSession;

public class SparkSessionClass {

	private static SparkSession spark;

	private SparkSessionClass() {

	}

	public static SparkSession getInstance() {
		if (spark == null)
			spark = getSparkSession();
		
		return spark;
	}

	public static SparkSession getSparkSession() {

		SparkSession sparkSession = SparkSession.builder().

//				config("hbase.zookeeper.quorum", "/src/main/resources/hbase-site.xml")
//				.config("hbase.zookeeper.property.clientPort", 2181)
				//config.set("spark.driver.allowMultipleContexts", "true");
	               //.config.addResource(new Path("/etc/hbase/conf/core-site.xml"));
	               //.config.addResource(new Path("/etc/hbase/conf/hbase-site.xml"));
			//	.config("hive.metastore.uris", "thrift://192.168.1.111:9083")
				
				appName("appName").master("local")
				//.enableHiveSupport()
				
				.getOrCreate();
		
		
		return sparkSession;
	}

}
