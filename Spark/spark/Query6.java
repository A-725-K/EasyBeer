package spark;

import scala.Tuple2;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.SparkSession;

import java.lang.Math;
import java.util.Map;
import java.util.Arrays;
import java.util.List;
import java.util.regex.Pattern;

public final class Query6 {
	public static final Pattern COMMA = Pattern.compile(",");
	public static final String DATASET = "hdfs://master:9000/user/user10/input/beers.csv";

	private static Tuple2<Tuple2<String, String>, Double> min = new Tuple2<>(new Tuple2<>("", ""), Double.MAX_VALUE);

	//compute the mean of a collection of double
	public static Double mean(Iterable<Double> m) {
		double sum = 0;
		double size = 0;

		for (double i : m) {
			size++;
			sum += i;
		}
		
		return sum / size;
	}

	//pairs:
	//	K  ==>  a pair of beer's styles
	//  V  ==>  how much their average abv differ
	private static void setMin(List<Tuple2<Tuple2<String, String>, Double>> map) {
		map.forEach( t  -> {
			if (t._2() <= min._2())
				min = t;
		});
	}

	//SHOW THE PAIR OF BEER'S STYLE WITH MOST SIMILAR ABV (IN AVERAGE)
	public static void main(String[] args) throws Exception {
		SparkSession ss = SparkSession
				.builder()
				.appName("Q6")
				.getOrCreate();

		JavaRDD<String> lines = ss.read().textFile(DATASET).javaRDD();	
		JavaPairRDD<String, Double> style_and_abv = lines.mapToPair(b -> new Tuple2<>(COMMA.split(b)[2], Double.valueOf(COMMA.split(b)[6]) ));
		JavaPairRDD<String, Double> style_and_average_abv = style_and_abv.groupByKey().mapValues(b -> mean(b));	
		JavaPairRDD<Tuple2<String, Double>, Tuple2<String, Double>> cartesian = style_and_average_abv
				.cartesian(style_and_average_abv)
				.filter(b -> !b._1()._1().equals(b._2()._1()));
		JavaPairRDD<Tuple2<String, String>, Double> couples = cartesian
				.mapToPair(b -> new Tuple2<>(new Tuple2<>(b._1()._1(), b._2()._1()), Math.abs(b._1()._2() - b._2()._2())));

		setMin(couples.collect());
        System.out.println(min._1()._1() + " and " + min._1()._2() + " are the beer's styles with more similar average ABV, the difference is " + min._2());
		ss.stop();
	}
}
