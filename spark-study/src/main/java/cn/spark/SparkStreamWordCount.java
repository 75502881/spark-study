package cn.spark;

import java.util.Arrays;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
public class SparkStreamWordCount {
	public static void main(String[] args) throws InterruptedException {
		SparkConf conf = new SparkConf().setAppName("ParallelizeConnection").setMaster("local");
		JavaStreamingContext jssc = new JavaStreamingContext(conf,Durations.seconds(1));
		
		JavaReceiverInputDStream<String> line = jssc.socketTextStream("localhost", 9999);
		
		
		jssc.start();
		jssc.awaitTermination();
		jssc.close();
		

	}
}
