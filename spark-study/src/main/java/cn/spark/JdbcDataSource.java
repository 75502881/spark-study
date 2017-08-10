package cn.spark;

import java.util.Properties;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;

import scala.collection.Seq;

public class JdbcDataSource {
	public static void main(String[] args) {

		String warehouseLocation = "/user/hive/warehouse";
		SparkSession spark = SparkSession.builder().appName("JdbcDataSource")
				.config("spark.sql.warehouse.dir", warehouseLocation).master("spark://192.168.31.231:7077")
				.getOrCreate();

		Dataset<Row> jdbcDF = getTableName(spark, "student_info");
		Dataset<Row> jdbcScoreDF = getTableName(spark, "student_scores");
		jdbcDF.show();
		System.out.println("---------------------");
		jdbcScoreDF.show();

		System.out.println("****************************");
		Seq<String> seq1 = new scala.collection.immutable.Set.Set1<String>("name").toSeq();
		Dataset<Row> dataFilter = jdbcScoreDF.filter(jdbcScoreDF.col("score").gt(80)).join(jdbcDF, seq1);
		dataFilter.show();

		Properties prop = new Properties();
		prop.put("user", "root");// 表示用户名是root
		prop.put("password", "oushuhua");// 表示密码是hadoop
		prop.put("driver", "com.mysql.jdbc.Driver");// 表示驱动程序是com.mysql.jdbc.Driver

		dataFilter.write().mode(SaveMode.Append).jdbc("jdbc:mysql://192.168.31.231:3306/testdb", "good_student", prop);

		spark.stop();

	}

	private static Dataset<Row> getTableName(SparkSession spark, String tableName) {
		Dataset<Row> jdbcDF = spark.read().format("jdbc").option("url", "jdbc:mysql://192.168.31.231:3306/testdb")
				.option("dbtable", tableName).option("user", "root").option("password", "oushuhua").load();
		return jdbcDF;
	}

}
