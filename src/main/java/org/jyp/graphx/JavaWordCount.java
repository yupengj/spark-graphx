package org.jyp.graphx;

import java.util.Arrays;
import java.util.List;
import java.util.regex.Pattern;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import scala.Tuple2;

public class JavaWordCount {
	private static final Pattern SPACE = Pattern.compile(" ");

	public static void main(String[] args) throws Exception {

//		if (args.length < 1) {
//			System.err.println("Usage: JavaWordCount <file>");
//			System.exit(1);
//		}

		String path = "file:///D:/git/spark-graphx/README.md";
		SparkConf conf = new SparkConf().setAppName("JavaWordCount").setMaster("local[2]");
		SparkContext sc = new SparkContext(conf);
		JavaRDD<String> lines = sc.textFile(path, 2).toJavaRDD();

		JavaRDD<String> words = lines.flatMap(s -> Arrays.asList(SPACE.split(s)).iterator());

		JavaPairRDD<String, Integer> ones = words.mapToPair(s -> new Tuple2<>(s, 1));

		JavaPairRDD<String, Integer> counts = ones.reduceByKey(Integer::sum);

		List<Tuple2<String, Integer>> output = counts.collect();
		for (Tuple2<?, ?> tuple : output) {
			System.out.println(tuple._1() + ": " + tuple._2());
		}
		sc.stop();
	}
}
