package cn.spark;



import java.io.IOException;
import java.net.URISyntaxException;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;

public class ManuallySepcifyOptions {

	public static void main(String[] args) throws IOException, URISyntaxException {		
		SparkSession sparkSession = SparkSession.builder().appName("Java Spark SQL basic example")
				.config("spark.master", "local").getOrCreate();

		Dataset<Row> df = sparkSession.read().load("hdfs://Hadoop-NameNode:9000/sparksql/users.parquet");
	
		
		df.printSchema();
		df.show();
		

		df.select("name","favorite_color").write().mode(SaveMode.Overwrite).save("hdfs://Hadoop-NameNode:9000/sparksql/nameAndFav.parquet");
	}

}
