package com.cpphot.action;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction2;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaPairReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;

import scala.Tuple2;

public class MainKafka {
	public static void main(String[] args) {
		SparkConf conf = new SparkConf().setMaster("local[2]").setAppName(
				"NetworkWordCount");

		JavaStreamingContext jssc = new JavaStreamingContext(conf,
				Durations.seconds(3));

		String kafkaZK = "cxp1:2181,cxp2:2181,cxp3:2181";
		String kafkaGroup = "group1";
		Map<String, Integer> topicPartitionNumMap = new HashMap<String, Integer>();
		topicPartitionNumMap.put("test", 1);
		JavaPairReceiverInputDStream<String, String> kafkaStream = KafkaUtils
				.createStream(jssc, kafkaZK, kafkaGroup, topicPartitionNumMap);

		JavaDStream<String> words = kafkaStream
				.flatMap(new FlatMapFunction<Tuple2<String, String>, String>() {

					@Override
					public Iterator<String> call(Tuple2<String, String> t)
							throws Exception {
						System.out.println(t._1+"~"+t._2);
						return Arrays.asList(t._2.split(" ")).iterator();
					}

				});

		JavaPairDStream<String, Integer> pairs = words
				.mapToPair(new PairFunction<String, String, Integer>() {
					public Tuple2<String, Integer> call(String s) {
						System.out.println("Tuple2[" + s + "]");
						return new Tuple2<String, Integer>(s, 1);
					}
				});

		JavaPairDStream<String, Integer> wordCounts = pairs
				.reduceByKey(new Function2<Integer, Integer, Integer>() {
					public Integer call(Integer i1, Integer i2) {
						System.out
								.println("reduceByKey[" + i1 + "," + i2 + "]");
						return i1 + i2;
					}
				});

		wordCounts.foreachRDD(new VoidFunction2() {

			public void call(Object arg0, Object arg1) throws Exception {
				System.out.println("foreachRDD[" + arg0 + "," + arg1 + "]");
			}
		});

		wordCounts.print();

		jssc.start(); // Start the computation
		try {
			jssc.awaitTermination();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}
}
