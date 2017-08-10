package cn.spark;

import java.io.IOException;
import java.net.URISyntaxException;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class ParquetDataDis {

	public static void main(String[] args) throws IOException, URISyntaxException {
		SparkSession sparkSession = SparkSession.builder().appName("Java Spark SQL basic example")
				.config("spark.master", "local").getOrCreate();

		Dataset<Row> df = sparkSession.read()
				.parquet("hdfs://Hadoop-NameNode:9000/user/");

		df.printSchema();
		df.show();
	}

}
