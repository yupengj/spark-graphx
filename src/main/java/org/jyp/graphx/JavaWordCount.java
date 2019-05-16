package org.jyp.graphx;

import java.util.List;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import scala.Tuple2;

public class JavaWordCount {
	private static final Pattern SPACE = Pattern.compile(" ");

	public static void main(String[] args) throws Exception {
		if (args.length < 1) {
			System.err.println("Usage: JavaWordCount <file>");
			System.exit(1);
		}
		SparkConf sparkConf = new SparkConf().setAppName("JavaWordCount").setMaster("local[*]");
		SparkContext spark = new SparkContext(sparkConf);
		JavaRDD<String> lines = spark.textFile(args[0], 2).toJavaRDD();
		JavaRDD<String> words = lines.flatMap(s -> Stream.of(SPACE.split(s)).collect(Collectors.toList()).iterator());
		JavaPairRDD<String, Integer> ones = words.mapToPair(s -> new Tuple2<>(s, 1));
		JavaPairRDD<String, Integer> counts = ones.reduceByKey(Integer::sum);
		JavaPairRDD<String, Integer> newCounts = counts.mapToPair(row -> new Tuple2<>(row._2, row._1)).sortByKey(false)
				.mapToPair(row -> new Tuple2<>(row._2, row._1));
		List<Tuple2<String, Integer>> output = newCounts.take(10);
		System.out.println(output.toString());
		spark.stop();
	}
}
