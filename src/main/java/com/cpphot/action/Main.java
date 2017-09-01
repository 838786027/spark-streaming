package com.cpphot.action;

import java.util.Arrays;
import java.util.Iterator;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction2;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

import scala.Tuple2;

public class Main {
	/**
	 * @param args
	 */
	public static void main(String[] args) {
		SparkConf conf = new SparkConf().setMaster("local[2]").setAppName(
				"NetworkWordCount");
		JavaStreamingContext jssc = new JavaStreamingContext(conf,
				Durations.seconds(3));

		JavaReceiverInputDStream<String> lines = jssc.socketTextStream(
				"cxp3", 9999);

		JavaDStream<String> words = lines
				.flatMap(new FlatMapFunction<String, String>() {
					public Iterator<String> call(String x) {
						System.out.println("FLATMAP["+x+"]");
						return Arrays.asList(x.split(" ")).iterator();
					}
				});

		JavaPairDStream<String, Integer> pairs = words
				.mapToPair(new PairFunction<String, String, Integer>() {
					public Tuple2<String, Integer> call(String s) {
						System.out.println("Tuple2["+s+"]");
						return new Tuple2<String, Integer>(s, 1);
					}
				});

		JavaPairDStream<String, Integer> wordCounts = pairs
				.reduceByKey(new Function2<Integer, Integer, Integer>() {
					public Integer call(Integer i1, Integer i2) {
						System.out.println("reduceByKey["+i1+","+i2+"]");
						return i1 + i2;
					}
				});
		
		wordCounts.foreachRDD(new VoidFunction2() {

			public void call(Object arg0, Object arg1) throws Exception {
				System.out.println("foreachRDD["+arg0+","+arg1+"]");
			}
		});

		wordCounts.print();
		
		jssc.start();              // Start the computation
		try {
			jssc.awaitTermination();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}
}
