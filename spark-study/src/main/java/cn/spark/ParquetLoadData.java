package cn.spark;



import java.io.IOException;
import java.net.URISyntaxException;
import java.util.List;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class ParquetLoadData {

	public static void main(String[] args) throws IOException, URISyntaxException {		
		SparkSession sparkSession = SparkSession.builder().appName("Java Spark SQL basic example")
				.config("spark.master", "local").getOrCreate();

		Dataset<Row> df = sparkSession.read().parquet("hdfs://Hadoop-NameNode:9000/sparksql/users.parquet");
	
		df.createOrReplaceTempView("users");

		
		Dataset<Row> selectData=sparkSession.sql("select * from users");
		List<Row> dataRow=selectData.javaRDD().collect();
		for(Row row : dataRow){
 			System.out.println(row.toString());
		}
	}

}
