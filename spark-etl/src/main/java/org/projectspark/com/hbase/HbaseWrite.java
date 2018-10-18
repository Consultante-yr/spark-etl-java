package org.projectspark.com.hbase;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutionException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapred.TableOutputFormat;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapreduce.Job;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.Dataset;
import org.projectspark.com.config.Config;

import scala.Tuple2;

public class HbaseWrite extends org.projectspark.com.hbase.Writer {

	public HbaseWrite(Config appConfig) {
		super(appConfig);
		// TODO Auto-generated constructor stub
	}

	public static Configuration config() {

		Configuration conf = HBaseConfiguration.create();
		conf.addResource(Thread.currentThread().getContextClassLoader().getResource("hbase-site.xml").getPath());
		conf.set(TableInputFormat.INPUT_TABLE, "user1");
		conf.set(TableInputFormat.SCAN_COLUMN_FAMILY, "default");
		conf.set(TableInputFormat.SCAN_COLUMNS, "default:firstName,default:lastName");
		return conf;

	}

	@Override
	public void WriteRDD() throws ExecutionException, IOException {
		Configuration conf = HbaseWrite.config();

		Job newAPIJobConfiguration1 = Job.getInstance(conf);
		newAPIJobConfiguration1.getConfiguration().set(TableOutputFormat.OUTPUT_TABLE, "user1");
		newAPIJobConfiguration1.setOutputFormatClass(org.apache.hadoop.hbase.mapreduce.TableOutputFormat.class);

		JavaSparkContext jsc = new JavaSparkContext(spark.sparkContext());

		List data = Arrays.asList("user-from-java-018,Jim,TonicJAVARDD1", "user-from-java-011,John,Davis",
				"user-from-java-012,Dave,Hill", "user-from-java-013,Ally,Deanon", "user-from-java-014,Joel,Dupont");
		JavaRDD<String> rdd1 = jsc.parallelize(data);

		JavaRDD<User> userRdd = rdd1.map(line -> {

			String[] userFields = line.split(",");
			User user = new User();
			user.setRowkey(userFields[0]);
			user.setFirstName(userFields[1]);
			user.setLastName(userFields[2]);
			return user;

		});

		JavaPairRDD<ImmutableBytesWritable, Put> tablePuts = userRdd.mapToPair(user -> {
			Put put = new Put(Bytes.toBytes(user.getRowkey()));
			put.addColumn(Bytes.toBytes("default"), Bytes.toBytes("firstName"), Bytes.toBytes(user.getFirstName()));
			put.addColumn(Bytes.toBytes("default"), Bytes.toBytes("lastName"), Bytes.toBytes(user.getLastName()));
			return new Tuple2<ImmutableBytesWritable, Put>(new ImmutableBytesWritable(), put);
		});
		tablePuts.saveAsNewAPIHadoopDataset(newAPIJobConfiguration1.getConfiguration());
		// tablePuts.saveas

		jsc.close();
	}

	@Override
	public void WriteDataFrame() throws ExecutionException, IOException {
		Configuration conf = HbaseWrite.config();

		Job newAPIJobConfiguration1 = Job.getInstance(conf);
		newAPIJobConfiguration1.getConfiguration().set(TableOutputFormat.OUTPUT_TABLE, "user1");
		newAPIJobConfiguration1.setOutputFormatClass(org.apache.hadoop.hbase.mapreduce.TableOutputFormat.class);

		JavaSparkContext jsc = new JavaSparkContext(spark.sparkContext());

		List data = Arrays.asList("user-from-java-010,Jim,Tonic", "user-from-java-011,John,Davis",
				"user-from-java-012,Dave,Hill", "user-from-java-013,Ally,Deanon", "user-from-java-014,Joel,Dupont");
		JavaRDD<String> rdd1 = jsc.parallelize(data);

		JavaRDD<User> userRdd = rdd1.map(line -> {

			String[] userFields = line.split(",");
			User user = new User();
			user.setRowkey(userFields[0]);
			user.setFirstName(userFields[1]);
			user.setLastName(userFields[2]);
			return user;

		});

		JavaPairRDD<ImmutableBytesWritable, Put> tablePuts = userRdd.mapToPair(user -> {
			Put put = new Put(Bytes.toBytes(user.getRowkey()));
			put.addColumn(Bytes.toBytes("default"), Bytes.toBytes("firstName"), Bytes.toBytes(user.getFirstName()));
			put.addColumn(Bytes.toBytes("default"), Bytes.toBytes("lastName"), Bytes.toBytes(user.getLastName()));
			return new Tuple2<ImmutableBytesWritable, Put>(new ImmutableBytesWritable(), put);
		});
		tablePuts.saveAsNewAPIHadoopDataset(newAPIJobConfiguration1.getConfiguration());
		jsc.close();
	}

	@Override
	public <T> void WriteDataSet() throws ExecutionException, IOException {
		Configuration conf = HbaseWrite.config();

		Job newAPIJobConfiguration1 = Job.getInstance(conf);
		newAPIJobConfiguration1.getConfiguration().set(TableOutputFormat.OUTPUT_TABLE, "user1");
		newAPIJobConfiguration1.setOutputFormatClass(org.apache.hadoop.hbase.mapreduce.TableOutputFormat.class);

		JavaSparkContext jsc = new JavaSparkContext(spark.sparkContext());

		List data = Arrays.asList("user-from-java-015,Jim,TonicDtaset", "user-from-java-011,JohnDataset,Davis",
				"user-from-java-012,Dave,Hill", "user-from-java-013,Ally,Deanon", "user-from-java-014,Joel,Dupont");
		JavaRDD<String> rdd1 = jsc.parallelize(data);

		JavaRDD<User> userRdd = rdd1.map(line -> {

			String[] userFields = line.split(",");
			User user = new User();
			user.setRowkey(userFields[0]);
			user.setFirstName(userFields[1]);
			user.setLastName(userFields[2]);
			return user;

		});

		JavaPairRDD<ImmutableBytesWritable, Put> tablePuts = userRdd.mapToPair(user -> {
			Put put = new Put(Bytes.toBytes(user.getRowkey()));
			put.addColumn(Bytes.toBytes("default"), Bytes.toBytes("firstName"), Bytes.toBytes(user.getFirstName()));
			put.addColumn(Bytes.toBytes("default"), Bytes.toBytes("lastName"), Bytes.toBytes(user.getLastName()));
			return new Tuple2<ImmutableBytesWritable, Put>(new ImmutableBytesWritable(), put);
		});
		tablePuts.saveAsNewAPIHadoopDataset(newAPIJobConfiguration1.getConfiguration());
		jsc.close();
	}

}
