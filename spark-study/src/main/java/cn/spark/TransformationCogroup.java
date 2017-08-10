package cn.spark;

import java.util.Arrays;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.VoidFunction;

import scala.Tuple2;

public class TransformationCogroup {
	public static void main(String[] args) {
		filter();
	}

	private static void filter() {
		SparkConf conf = new SparkConf().setAppName("ParallelizeConnection").setMaster("local");
		JavaSparkContext sc = new JavaSparkContext(conf);

		List<Tuple2<Integer, String>> nameList = Arrays.asList(new Tuple2<Integer, String>(1, "class1"),
				new Tuple2<Integer, String>(2, "class2"), new Tuple2<Integer, String>(3, "class3"),
				new Tuple2<Integer, String>(4, "class4"), new Tuple2<Integer, String>(8, "class8"));

		List<Tuple2<Integer, Integer>> scoresList = Arrays.asList(new Tuple2<Integer, Integer>(1, 10),
				new Tuple2<Integer, Integer>(9, 99), new Tuple2<Integer, Integer>(1, 30),
				new Tuple2<Integer, Integer>(1, 50), new Tuple2<Integer, Integer>(2, 55),
				new Tuple2<Integer, Integer>(2, 22), new Tuple2<Integer, Integer>(3, 34),
				new Tuple2<Integer, Integer>(4, 67));
		JavaPairRDD<Integer, String> nameListRdd = sc.parallelizePairs(nameList, 1);
		JavaPairRDD<Integer, Integer> scoresListRdd = sc.parallelizePairs(scoresList);
		JavaPairRDD<Integer, Tuple2<Iterable<String>, Iterable<Integer>>> studentListRdd = nameListRdd
				.cogroup(scoresListRdd).sortByKey();

		studentListRdd.foreach(new VoidFunction<Tuple2<Integer, Tuple2<Iterable<String>, Iterable<Integer>>>>(

		) {

			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			@Override
			public void call(Tuple2<Integer, Tuple2<Iterable<String>, Iterable<Integer>>> arg0) throws Exception {
				// TODO Auto-generated method stub
				System.out.println("id:" + arg0._1 + " class:" + arg0._2._1 + " score:" + arg0._2._2);
			}
		});

		sc.close();
	}
}
