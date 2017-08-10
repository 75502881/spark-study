package cn.spark;

import java.util.List;

import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import cn.bean.Student;

public class HiveDataSource {
	public static void main(String[] args) {

		String warehouseLocation = "/user/hive/warehouse";
		SparkSession spark = SparkSession.builder().appName("HiveDataSource")
				.config("spark.sql.warehouse.dir", warehouseLocation).master("spark://192.168.31.231:7077").enableHiveSupport().getOrCreate();

		// student_infos
		spark.sql("DROP TABLE IF EXISTS student_infos");
		spark.sql("CREATE TABLE IF NOT EXISTS student_infos (name STRING,age INT)");
		spark.sql("load data inpath 'hdfs://192.168.31.231:9000/81/student_infos.txt' into table student_infos");

		// student_score
		spark.sql("DROP TABLE IF EXISTS student_scores");
		spark.sql("CREATE TABLE IF NOT EXISTS student_scores (name STRING,score INT)");
		spark.sql("load data inpath 'hdfs://192.168.31.231:9000/81/student_scores.txt' into table student_scores");

		spark.sql("select * from student_infos").show();

		Encoder<Student> personEncoder = org.apache.spark.sql.Encoders.bean(Student.class);
		Dataset<Row> students = spark
				.sql("select si.name,si.age,ss.score from student_infos si join student_scores ss on si.name = ss.name where ss.score>=80");

		System.out.println("--------------------");


		students.write().saveAsTable("good_student");
		
		
		Dataset<Student> studentSet = students.map(new MapFunction<Row, Student>() {

			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			@Override
			public Student call(Row arg0) throws Exception {
				// TODO Auto-generated method stub
				Student stBean = new Student();
				stBean.setName(arg0.getAs("name"));
				stBean.setAge(arg0.getAs("age"));
				stBean.setScore(arg0.getAs("score"));
				System.out.println(
						"name:" + stBean.getName() + " age:" + stBean.getAge() + " socre:" + stBean.getScore());
				return stBean;
			}
		}, personEncoder);
		List<Student> studentssList = studentSet.collectAsList();
		System.out.println("type student info:");
		for (Student t : studentssList) {
			System.out.println("name:" + t.getName() + " age:" + t.getAge() + " socre:" + t.getScore());

		}
	}

}
