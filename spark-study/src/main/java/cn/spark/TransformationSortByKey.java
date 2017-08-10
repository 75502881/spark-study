package cn.spark;

import java.util.Arrays;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.VoidFunction;

import scala.Tuple2;

public class TransformationSortByKey {
	public static void main(String[] args) {
		filter();
	}

	private static void filter() {
		SparkConf conf = new SparkConf().setAppName("ParallelizeConnection").setMaster("local");
		JavaSparkContext sc = new JavaSparkContext(conf);

		List<Tuple2< Integer,String>> scoresList = Arrays.asList(new Tuple2< Integer,String>(54,"class1"),
				new Tuple2< Integer,String>(25,"class2"), new Tuple2< Integer,String>(44,"class3"),
				new Tuple2< Integer,String>( 65,"class1"));
		JavaPairRDD< Integer,String> scores = sc.parallelizePairs(scoresList, 1);
		JavaPairRDD< Integer,String> scoresGroup =scores.sortByKey(false);
		scoresGroup.foreach(new VoidFunction<Tuple2<Integer,String>>() {
			
			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			@Override
			public void call(Tuple2< Integer,String> arg0) throws Exception {
				// TODO Auto-generated method stub
				System.out.println("socre:"+arg0._1+" class:"+arg0._2);
			}
		});
	
		sc.close();
	}
}
