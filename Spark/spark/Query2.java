package spark;

import scala.Tuple2;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.SparkSession;

import java.util.Map;
import java.util.List;
import java.util.regex.Pattern;

public final class Query2 {
	public static final Pattern COMMA = Pattern.compile(",");
	public static final String DATASET_BEERS = "hdfs://master:9000/user/user10/input/beers.csv";
	public static final String DATASET_COLORS = "hdfs://master:9000/user/user10/input/colors.csv";
	
	private static Tuple2<Integer, Long> max_color = new Tuple2<>(-1, 0l);

	//map:
	//	K  ==>  color
	//  V  ==>  how many beers with that color
	private static void setMax(Map<Integer, Long> map) {
		map.forEach((k, v) -> {
			if (v >= max_color._2())
				max_color = new Tuple2<>(k, v);
		});
	}

	//SELECT THE MOST FREQUENT COLOR AND THE BEERS HAVING THAT COLOR
	public static void main(String[] args) throws Exception {
		SparkSession ss = SparkSession
				.builder()
				.appName("Q2")
				.getOrCreate();

		JavaRDD<String> lines_beers = ss.read().textFile(DATASET_BEERS).javaRDD();
		JavaRDD<String> lines_colors = ss.read().textFile(DATASET_COLORS).javaRDD();
		JavaRDD<Integer> only_colors = lines_beers.map(s -> new Double(COMMA.split(s)[8]).intValue());
		JavaPairRDD<Integer, String> beers_and_names = lines_beers
				.mapToPair(s -> new Tuple2<>(new Double(COMMA.split(s)[8]).intValue(), COMMA.split(s)[1]));
		JavaPairRDD<Integer, String> colors_and_names = lines_colors
				.mapToPair(s -> new Tuple2<>(new Double(COMMA.split(s)[0]).intValue(), COMMA.split(s)[1]));	
		Map<Integer, Long> how_many_beers_per_color = only_colors.countByValue();
		setMax(how_many_beers_per_color);
		String color_name = colors_and_names.lookup(max_color._1()).get(0);
		//The access of a field in a Tuple2 is lazy and cannot be done inside the lambda function
		int maxC = max_color._1().intValue();
		JavaPairRDD<Integer, String> only_names = beers_and_names.filter(b -> b._1().intValue() == maxC);
		List<Tuple2<Integer, String>> only_beers = only_names.collect();
		System.out.println("There are " + max_color._2() + " beers of color: " + color_name.toUpperCase());
		System.out.println("Those are:");
		for (Tuple2<?,?> tuple : only_beers)
			System.out.println(tuple._2());
		
		ss.stop();
	}
}
