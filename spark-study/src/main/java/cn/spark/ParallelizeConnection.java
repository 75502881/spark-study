package cn.spark;

import java.util.Arrays;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;

public class ParallelizeConnection {
	public static void main(String[] args) {
		SparkConf conf = new SparkConf().setAppName("ParallelizeConnection").setMaster("local");
		JavaSparkContext sc = new JavaSparkContext(conf);

		 
		List<Integer> numbers = Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10);
		JavaRDD<Integer> numberRdd = sc.parallelize(numbers);
		int sum = numberRdd.reduce(new Function2<Integer, Integer, Integer>() {

			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			@Override
			public Integer call(Integer arg0, Integer arg1) throws Exception {
				// TODO Auto-generated method stub
				return arg0 + arg1;
			}
		});
		System.out.println(sum);
		sc.close();

	}
}
