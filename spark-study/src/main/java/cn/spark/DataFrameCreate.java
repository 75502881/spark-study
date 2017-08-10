package cn.spark;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class DataFrameCreate {
	public static void main(String[] args) {

		SparkSession sparkSession = SparkSession.builder().appName("Java Spark SQL basic example")
				.config("spark.master", "local").getOrCreate();

		Dataset<Row> df = sparkSession.read().json("hdfs://Hadoop-NameNode:9000/sparksql/students.json");
		df.show();

		df.printSchema();
		System.out.println("----------------------------------------------");

		df.select("name").show();
		System.out.println("----------------------------------------------");

		df.select(df.col("name"), df.col("age").plus(1)).show();
		System.out.println("----------------------------------------------");

		df.filter(df.col("age").gt(21)).show();
		System.out.println("----------------------------------------------");

		df.groupBy("age").count().show();

	}
}
