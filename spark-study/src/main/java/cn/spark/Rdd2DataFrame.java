package cn.spark;

import java.util.ArrayList;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

public class Rdd2DataFrame {
	public static void main(String[] args) {

		// 创建SparkConf、JavaSparkContext、SQLContext
		SparkConf conf = new SparkConf().setMaster("local").setAppName("RDD2DataFrameProgrammatically");
		JavaSparkContext sc = new JavaSparkContext(conf);

		SparkSession sparkSession = SparkSession.builder().appName("Java Spark SQL basic example")
				.config("spark.master", "local").getOrCreate();


		// 第一步，创建一个普通的RDD，但是，必须将其转换为RDD<Row>的这种格式
		JavaRDD<String> lines = sc.textFile("hdfs://Hadoop-NameNode:9000/sparksql/students.txt");

		JavaRDD<Row> studentRDD = lines.map(line -> {
			String[] lineSplited = line.split(",");
			Row rows= RowFactory.create(Integer.valueOf(lineSplited[0]), lineSplited[1], Integer.valueOf(lineSplited[2]));
			return rows;
		});
		
		
//		JavaRDD<Row> studentRDD = lines.map(new Function<String, Row>(
//				) {
//
//					/**
//					 * 
//					 */
//					private static final long serialVersionUID = 1L;
//
//					@Override
//					public Row call(String line) throws Exception {
//						String[] lineSplited = line.split(",");
//						return RowFactory.create(Integer.valueOf(lineSplited[0]), lineSplited[1], Integer.valueOf(lineSplited[2]));
//					}
//		});
		// 第二步，动态构造元数据
		// 比如说，id、name等，field的名称和类型，可能都是在程序运行过程中，动态从mysql db里
		// 或者是配置文件中，加载出来的，是不固定的
		// 所以特别适合用这种编程的方式，来构造元数据
		List<StructField> structFields = new ArrayList<StructField>();
		structFields.add(DataTypes.createStructField("id", DataTypes.IntegerType, true));
		structFields.add(DataTypes.createStructField("name", DataTypes.StringType, true));
		structFields.add(DataTypes.createStructField("age", DataTypes.IntegerType, true));
		StructType structType = DataTypes.createStructType(structFields);

		// 第三步，使用动态构造的元数据，将RDD转换为DataFrame
		Dataset<Row> studentDF = sparkSession.createDataFrame(studentRDD, structType);

		// 后面，就可以使用DataFrame了
		studentDF.createOrReplaceTempView("students");

		Dataset<Row> teenagerDF = sparkSession.sql("select * from students where age<18");

		List<Row> rows = teenagerDF.javaRDD().collect();
		for (Row row : rows) {
			System.out.println(row);
		}
		
		sc.close();
	}
}
