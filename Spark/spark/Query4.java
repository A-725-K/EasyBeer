package spark;

import scala.Tuple2;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.SparkSession;

import java.util.Arrays;
import java.util.List;
import java.util.regex.Pattern;

public final class Query4 {
	public static final Pattern COMMA = Pattern.compile(",");
	public static final String DATASET = "hdfs://master:9000/user/user10/input/beers.csv";
	public static final String BREW_METHOD = "BIAB";

	//Among all beers that share a brew method (e.g. BIAB), compute the average of apparent
	//attenuation (in percentage) and the average real attenuation (in percentage)
	//see www.brewtheplanet.it/grado_alcolico.html
	public static void main(String[] args) throws Exception {
		SparkSession ss = SparkSession
				.builder()
				.appName("Q4")
				.getOrCreate();

		JavaRDD<String>	lines = ss.read().textFile(DATASET).javaRDD();
		JavaPairRDD<Double, Double> og_fg = lines
				.filter(s -> COMMA.split(s)[13].equals(BREW_METHOD))
				.mapToPair(s -> new Tuple2<>(new Double(COMMA.split(s)[4]), new Double(COMMA.split(s)[5])));
		JavaRDD<Double> diff = og_fg.map(t -> (t._1() - t._2())/(t._1() - 1)*100);
		
		double sum = diff.reduce((x1, x2) -> x1 + x2);
		long N = diff.count();
		double mean_aa = sum/N;
		double mean_ra = mean_aa/1.22;
		
		System.out.println("Brew method: " + BREW_METHOD);
		System.out.println("In average, the average Apparent Attenuation(AA%) is: " + mean_aa + " %");
		System.out.println("In average, the average Real Attenuation(RA%) is: " + mean_ra + " %");
		ss.stop();
	}
}
