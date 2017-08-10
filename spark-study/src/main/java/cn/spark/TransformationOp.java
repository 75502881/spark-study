package cn.spark;

import java.util.Arrays;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;

public class TransformationOp {
	public static void main(String[] args) {
		filter();
		map();
	}

	private static void map() {
		SparkConf conf = new SparkConf().setAppName("ParallelizeConnection").setMaster("local");
		JavaSparkContext sc = new JavaSparkContext(conf);

		List<Integer> numbers = Arrays.asList(1, 2, 34, 54, 78, 90);
		JavaRDD<Integer> numberRdd = sc.parallelize(numbers);
		JavaRDD<Integer> multNumberRdd = numberRdd.map(new Function<Integer, Integer>() {

			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			@Override
			public Integer call(Integer arg0) throws Exception {
				// TODO Auto-generated method stub
				return arg0 * 2;
			}
		});

		multNumberRdd.foreach(new VoidFunction<Integer>() {

			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			@Override
			public void call(Integer arg0) throws Exception {
				System.out.println(arg0);

			}
		});
		sc.close();
	}

	private static void filter() {
		SparkConf conf = new SparkConf().setAppName("ParallelizeConnection").setMaster("local");
		JavaSparkContext sc = new JavaSparkContext(conf);

		List<Integer> numbers = Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10);
		JavaRDD<Integer> numberRdd = sc.parallelize(numbers);
		JavaRDD<Integer> multNumberRdd = numberRdd.filter(new Function<Integer, Boolean>() {

			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			@Override
			public Boolean call(Integer arg0) throws Exception {
				// TODO Auto-generated method stub
				if (arg0 % 2 == 0)
					return false;
				else
					return true;
			}
		});

		multNumberRdd.foreach(new VoidFunction<Integer>() {

			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			@Override
			public void call(Integer arg0) throws Exception {
				System.out.println(arg0);

			}
		});
		sc.close();
	}
}
