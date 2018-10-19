package org.projectspark.com.hbase;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.rdd.RDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.projectspark.com.config.Config;
import org.projectspark.com.elasticsearch.EsReader;
import org.projectspark.com.elasticsearch.JsonAdapter;
import org.projectspark.com.util.SparkSessionClass;

import org.apache.spark.api.java.function.Function;
import org.apache.hadoop.hbase.util.Bytes;
import scala.Tuple2;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;

import org.apache.spark.sql.SQLContext;
import scala.Tuple2;

public class HbaseReader extends org.projectspark.com.hbase.Reader {

	public HbaseReader(Config appConfig) {
		super(appConfig);
		// TODO Auto-generated constructor stub
	}

	public static Configuration config() {

		Configuration conf = HBaseConfiguration.create();
		conf.addResource(Thread.currentThread().getContextClassLoader().getResource("hbase-site.xml").getPath());

		// conf.set(TableInputFormat.INPUT_TABLE, "tt");
		// conf.set(TableInputFormat.SCAN_COLUMN_FAMILY, "cf2");
		// conf.set(TableInputFormat.SCAN_COLUMN_FAMILY, "cf1");
		// conf.set(TableInputFormat.SCAN_COLUMNS, "cf1:id cf2:name cf2:age");

		conf.set(TableInputFormat.INPUT_TABLE, "user1");
		conf.set(TableInputFormat.SCAN_COLUMN_FAMILY, "default");
		conf.set(TableInputFormat.SCAN_COLUMNS, "default:firstName,default:lastName");
		return conf;

	}

	public JavaRDD<?> readRDD() {

		Configuration conf = HbaseReader.config();

		RDD<Tuple2<ImmutableBytesWritable, Result>> hBaseRDD = SparkSessionClass.getSparkSession().sparkContext()
				.newAPIHadoopRDD(conf, TableInputFormat.class, ImmutableBytesWritable.class, Result.class);

		return hBaseRDD.toJavaRDD();
	}
	
	
	public Dataset<Row> readDataFrame() throws NoSuchMethodException, SecurityException, InstantiationException, IllegalAccessException, IllegalArgumentException, InvocationTargetException {

		//Class<?> clazz = User.class;
	
	//	String rowkey_conf = (String) appConfig.getEntry("rowkey.name");
		String Colonne_famille = (String) appConfig.getEntry("Colonne_famille.name");
		String Colonne_qualifier1 = (String) appConfig.getEntry("Colonne_qualifier.name1");
		String Colonne_qualifier2 = (String) appConfig.getEntry("Colonne_qualifier.name2");
//		Class<?> clazz = (Class<?>) appConfig.getEntry("name_class.class");
//		String className = clazz.getName();
//		Constructor<?> cons = clazz.getConstructor(String.class);
//		
//		cons.setAccessible(true);
		
		//Object obj = cons.newInstance();
		Configuration conf = config();

		JavaSparkContext jsc = new JavaSparkContext(spark.sparkContext());

		JavaPairRDD<ImmutableBytesWritable, Result> hBaseRDD = jsc.newAPIHadoopRDD(conf, TableInputFormat.class,
				ImmutableBytesWritable.class, Result.class);

		JavaPairRDD rowPairRDD = hBaseRDD.mapToPair(entry -> {
			Class<?> clazz = (Class<?>) appConfig.getEntry("name_class.class");
			String className = clazz.getName();
//			Constructor<?> cons = clazz.getConstructor(String.class);
//			
//			cons.setAccessible(true);
//			
//			Object obj = cons.newInstance();
//			
			Result r = entry._2;
			String rowKey = Bytes.toString(r.getRow());
			clazz cd = new clazz();
			
			//User user = new User();
		//	cd.setRowkey(rowKey);
			cd.setFirstName(Bytes.toString(r.getValue(Bytes.toBytes(Colonne_famille), Bytes.toBytes(Colonne_qualifier1))));
			cd.setLastName(Bytes.toString(r.getValue(Bytes.toBytes(Colonne_famille), Bytes.toBytes(Colonne_qualifier2))));

			// user.setAge(Integer.valueOf(Bytes.toString(r.getValue(Bytes.toBytes("default"),
			// Bytes.toBytes("age")))));

			return new Tuple2(rowKey, cd);

		});

		System.out.println("************ RDD *************");
		System.out.println(rowPairRDD.count());
		System.out.println(rowPairRDD.keys().collect());
		System.out.println(rowPairRDD.values().collect());

		System.out.println("************ DF *************");
		Dataset<Row> df = spark.createDataFrame(rowPairRDD.values(), User.class);

		System.out.println(df.count());
		System.out.println(df.schema());
		df.show();

		System.out.println("************ DF with SQL *************");
		df.registerTempTable("USER_TABLE");
		Dataset<Row> dfSql = spark.sql("SELECT *  FROM USER_TABLE  WHERE firstName = 'Ally' ");
		// System.out.println(dfSql.count());
		// System.out.println(dfSql.schema());
		// dfSql.show();

		// jsc.close();

		return dfSql;

	}

	
	
	public int  function2() {
		return 0;
		
	}
	@Override
	public <T> Dataset<T> readDataSet(Class<T> klass) throws NoSuchMethodException, SecurityException, InstantiationException, IllegalAccessException, IllegalArgumentException, InvocationTargetException {
		readDataFrame().as(Encoders.bean(klass));
		return null;
	}

}
