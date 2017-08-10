package cn.spark;

import java.util.Arrays;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.broadcast.Broadcast;
public class BroadcastVar {
	public static void main(String[] args) {
		SparkConf conf = new SparkConf().setAppName("ParallelizeConnection").setMaster("local");
		JavaSparkContext sc = new JavaSparkContext(conf);


		
		
		Broadcast<Integer> broadcast= sc.broadcast(4);

		

		List<Integer> numberList= Arrays.asList(1,2,3,4,5);
		
		JavaRDD<Integer> numRdd = sc.parallelize(numberList);
		
		JavaRDD<Integer> numDoubleRdd =numRdd.map(new Function<Integer, Integer>() {

			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			@Override
			public Integer call(Integer arg0) throws Exception {
				// TODO Auto-generated method stub
				return arg0*broadcast.getValue();
			}
		});
	
		numDoubleRdd.foreach(new VoidFunction<Integer>() {
			
			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			@Override
			public void call(Integer arg0) throws Exception {
				// TODO Auto-generated method stub
				System.out.println(arg0);
			}
		});
		sc.close();

	}
}
