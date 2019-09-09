package spark;

import scala.Tuple2;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.SparkSession;

import java.util.Arrays;
import java.util.List;
import java.util.regex.Pattern;

public final class Query {
	public static final Pattern COMMA = Pattern.compile(",");
	public static final String DATASET = "hdfs://master:9000/user/user10/input/beers.csv";

	//
	public static void main(String[] args) throws Exception {
		SparkSession ss = SparkSession
				.builder()
				.appName("Q")
				.getOrCreate();

		
		ss.stop();
	}
}
