package cn.spark;

import java.io.IOException;
import java.net.URISyntaxException;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class GenericLoadSave {

	public static void main(String[] args) throws IOException, URISyntaxException {
		
		Path output = new Path("hdfs://Hadoop-NameNode:9000/sparksql/nameAndFav.parquet");  
		FileSystem hdfs = org.apache.hadoop.fs.FileSystem.get(  
		      new java.net.URI("hdfs://Hadoop-NameNode:9000/"), new org.apache.hadoop.conf.Configuration());
		hdfs.listFiles(output, true);
		if (hdfs.exists(output)) hdfs.delete(output, true);
		
		SparkSession sparkSession = SparkSession.builder().appName("Java Spark SQL basic example")
				.config("spark.master", "local").getOrCreate();

		Dataset<Row> df = sparkSession.read().load("hdfs://Hadoop-NameNode:9000/sparksql/users.parquet");
		df.printSchema();
		df.show();
		
		df.select("name","favorite_color").write().save("hdfs://Hadoop-NameNode:9000/sparksql/nameAndFav.parquet");
	}

}
