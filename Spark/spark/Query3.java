package spark;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.SparkSession;

import java.lang.Math;
import java.util.regex.Pattern;

public final class Query3 {
	public static final Pattern COMMA = Pattern.compile(",");
	public static final String DATASET = "hdfs://master:9000/user/user10/input/beers.csv";
	public static final String STYLE = "American IPA";

	//GIVEN A STYLE(e.g. American IPA) COMPUTE THE AVERAGE AND THE STANDARD DEVIATION OF THE COLOR
	public static void main(String[] args) throws Exception {
		SparkSession ss = SparkSession
				.builder()
				.appName("Q3")
				.getOrCreate();

		JavaRDD<String>	lines = ss.read().textFile(DATASET).javaRDD();
		JavaRDD<Double> colors = lines
				.filter(s -> COMMA.split(s)[2].equals(STYLE)).map(s -> new Double(COMMA.split(s)[8]));
		double sum = colors.reduce((x1, x2) -> x1 + x2);
		long N = colors.count();
		double mean = sum/N;
		JavaRDD<Double> num_std = colors.map(x -> Math.pow(x - mean, 2.0));
		double sum_std = num_std.reduce((x1, x2) -> x1 + x2);
		double std = Math.sqrt(sum_std / N);
		System.out.println("STYLE: " + STYLE);
		System.out.println("Average: " + mean);
		System.out.println("STD deviation: " + std);
		ss.stop();
	}
}
