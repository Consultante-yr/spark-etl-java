package org.projectspark.com.elasticsearch;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
import org.apache.hadoop.yarn.webapp.hamlet.Hamlet.A;
import org.apache.spark.api.java.AbstractJavaRDDLike;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.rdd.RDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.elasticsearch.spark.rdd.api.java.JavaEsSpark;
import org.elasticsearch.spark.sql.api.java.JavaEsSparkSQL;
import org.projectspark.com.config.Config;
import org.projectspark.com.hbase.HbaseReader;
import org.projectspark.com.util.SparkSessionClass;
import org.spark_project.jetty.server.Authentication.User;

import java.util.List;

import scala.Tuple2;
import scala.collection.Seq;
import scala.reflect.api.TypeTags.TypeTag;

public class EsReader extends org.projectspark.com.reader.ReaderClass {

	public EsReader(Config appConfig) {
		super(appConfig);
		// TODO Auto-generated constructor stub
	}

	public static Configuration config() {

		Configuration conf = HBaseConfiguration.create();

		conf.set("es.index.auto.create", "true");
		conf.set("es.nodes", "127.0.0.1:9200");
		conf.set("spark.es.nodes.wan.only", "true");

		// conf.set("es.index.auto.create", "true");

		return conf;

	}

	public JavaRDD<?> readRDD() {
		Configuration conf = EsReader.config();
		//
		JavaSparkContext jsc = new JavaSparkContext(spark.sparkContext());
		//
		JavaPairRDD<String, String> esRDD = JavaEsSpark.esJsonRDD(jsc, "spark1/docs");
		JavaRDD newRDD = esRDD.values();

		// Quelles données avez-vous dans les colonnes JavaPairRDD? Un JavaPairRDD est
		// un mappage clé/valeur entre la première et la deuxième colonne, contrairement
		// à un RDD normal.
		//
		// Vous souhaitez éventuellement supprimer la première colonne du JavaPairRDD,
		// en renvoyant uniquement JavaRDD avec uniquement la colonne de valeur.

		return newRDD;

	}
	// }
	//
	// public static final Function<Tuple2<String, String>, Row> mappingFunc =
	// (tuple) -> {
	// return RowFactory.create(tuple._1(), tuple._2());
	// };

	//
	public Dataset<Row> readDataFrame() {

		JavaSparkContext jsc = new JavaSparkContext(spark.sparkContext());
		JavaPairRDD<String, String> esRDD = JavaEsSpark.esJsonRDD(jsc, "spark1/docs");
		Class<?> clazz = (Class<?>) appConfig.getEntry("input.mapping.class");

		return spark.createDataFrame(esRDD.values().map(j -> {
			return JsonAdapter.Deserialize(j, clazz);
		}), clazz);

	}

	@Override
	public <T> Dataset<T> readDataSet(Class<T> klass) {
		JavaSparkContext jsc = new JavaSparkContext(spark.sparkContext());
		JavaPairRDD<String, String> esRDD = JavaEsSpark.esJsonRDD(jsc, "spark1/docs");
		Class<T> clazz = (Class<T>) appConfig.getEntry("input.mapping.class");

		return spark.createDataFrame(esRDD.values().map(j -> {
			return JsonAdapter.Deserialize(j, clazz);
		}), clazz).as(Encoders.bean(clazz));
	}

}
