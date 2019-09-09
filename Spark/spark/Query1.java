package spark;

import scala.Tuple2;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.SparkSession;

import java.util.Arrays;
import java.util.List;
import java.util.regex.Pattern;

public final class Query1 {
	public static final Pattern COMMA = Pattern.compile(",");
	public static final String DATASET = "hdfs://master:9000/user/user10/input/beers.csv";

	//FOR EACH STYLE SELECT THE GREATEST ABV
	public static void main(String[] args) throws Exception {
		SparkSession ss = SparkSession
				.builder()
				.appName("Q1")
				.getOrCreate();

		JavaRDD<String> lines = ss.read().textFile(DATASET).javaRDD();
		JavaPairRDD<String, Double> abvs_per_beer = lines
				.mapToPair(b -> new Tuple2<>(COMMA.split(b)[2], Double.valueOf(COMMA.split(b)[6])));
		JavaPairRDD<String, Double> max_per_style = abvs_per_beer
				.reduceByKey((abv1, abv2) -> abv1 >= abv2 ? abv1 : abv2);

		List<Tuple2<String, Double>> result = max_per_style.collect();
		System.out.println("Maximum ABV per style");
		System.out.println("Style\tABV");
		for (Tuple2<?,?> tuple : result)
			System.out.println(tuple._1() + "\t" + tuple._2());
		
		ss.stop();
	}
}
