package org.projectspark.com.elasticsearch;


import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.elasticsearch.spark.rdd.api.java.JavaEsSpark;
import org.elasticsearch.spark.sql.api.java.JavaEsSparkSQL;
import org.projectspark.com.config.Config;
import org.projectspark.com.writer.WriteClass;

public class EsWrite extends WriteClass{

	public EsWrite(Config appConfig) {
		super(appConfig);
		// TODO Auto-generated constructor stub
	}


	@Override
	public void WriteRDD(JavaRDD<?> rdd)   {

		String index = (String) appConfig.getEntry("elastic.index");
		String type = (String) appConfig.getEntry("elastic.type");
		JavaEsSpark.saveJsonToEs((JavaRDD<String>) rdd,  index+"/"+type);

	}

	@Override
	public void WriteDataFrame(Dataset<Row> df)   {
		String index = (String) appConfig.getEntry("elastic.index");
		String type = (String) appConfig.getEntry("elastic.type");
		JavaEsSparkSQL.saveToEs(df, index+"/"+type);	
	}

	@Override
	public <T> void WriteDataSet(Dataset<?> ds)  {
		String index = (String) appConfig.getEntry("elastic.index");
		String type = (String) appConfig.getEntry("elastic.type");
		JavaEsSparkSQL.saveToEs(ds, index+"/"+type);
	}

}
