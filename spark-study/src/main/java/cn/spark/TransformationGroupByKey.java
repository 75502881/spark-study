package cn.spark;

import java.util.Arrays;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.VoidFunction;

import scala.Tuple2;

public class TransformationGroupByKey {
	public static void main(String[] args) {
		filter();
	}

	private static void filter() {
		SparkConf conf = new SparkConf().setAppName("ParallelizeConnection").setMaster("local");
		JavaSparkContext sc = new JavaSparkContext(conf);

		List<Tuple2<String, Integer>> scoresList = Arrays.asList(new Tuple2<String, Integer>("class1", 65),
				new Tuple2<String, Integer>("class2", 25), new Tuple2<String, Integer>("class1", 95),
				new Tuple2<String, Integer>("class1", 65), new Tuple2<String, Integer>("class2", 15));
		JavaPairRDD<String, Integer> scores = sc.parallelizePairs(scoresList, 1);
		JavaPairRDD<String, Integer> scoresGroup =scores.reduceByKey(new Function2<Integer, Integer, Integer>() {
			
			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			@Override
			public Integer call(Integer arg0, Integer arg1) throws Exception {
				// TODO Auto-generated method stub
				return arg0+arg1;
			}
		});
		scoresGroup.foreach(new VoidFunction<Tuple2<String,Integer>>() {
			
			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			@Override
			public void call(Tuple2<String, Integer> arg0) throws Exception {
				// TODO Auto-generated method stub
				System.out.println("class:"+arg0._1+"score:"+arg0._2);
			}
		});
	
		sc.close();
	}
}
