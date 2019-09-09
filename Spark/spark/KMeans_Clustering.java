package spark;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.SparkSession;

import org.apache.spark.mllib.clustering.KMeans;
import org.apache.spark.mllib.clustering.KMeansModel;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;

import java.util.Map;
import java.util.regex.Pattern;

import java.time.Instant;
import java.time.Duration;

public final class KMeans_Clustering {
	public static final Pattern COMMA = Pattern.compile(",");
	public static final String DATASET = "hdfs://master:9000/user/user10/input/clustering.csv";
	public static final String OUTPUT = "res_cl";

	//Clustering with K-Means algorithm
	public static void main(String[] args) throws Exception {
		SparkSession ss = SparkSession
				.builder()
				.appName("KMeans Clustering")
				.getOrCreate();

				
		JavaRDD<String> lines = ss.read().textFile(DATASET).javaRDD();
		JavaRDD<Vector> dataset = lines.map(s -> {
			String[] fields = s.split(",");
			int len = fields.length;
			double[] vals = new double[len];
			for (int i = 0; i < len; i++)
				vals[i] = Double.parseDouble(fields[i]);
			return Vectors.dense(vals);
		});
		dataset.cache();

		//Parameters of K-Means
		int number_of_clusters = 5;
		int number_of_iterations = 100;

		Instant start = Instant.now(); //start stopwatch
		KMeansModel clusters = KMeans
			.train(dataset.rdd(),
				   number_of_clusters,
				   number_of_iterations,
				   "k-means||");
		Instant finish = Instant.now(); //end stopwatch

		System.out.println("Cluster centers:");
		for (Vector cc : clusters.clusterCenters())
			System.out.println(cc);

		double cost = clusters.computeCost(dataset.rdd());
		System.out.println("Cost:\t" + cost);

		long timeElapsed = Duration.between(start, finish).toMillis();
		System.out.println("Time in millis:\t" + timeElapsed);
		System.out.println("Time in seconds:\t" + timeElapsed/1000);

		int i = 0;
		int min = 150;
		int max = 250;
		for (Vector v : dataset.collect()) {
			if (i++ > min && i < max)
				System.out.println("Point:\t" + v + "\t\t\t\t belongs to cluster " + clusters.predict(v));
			else if (i > max)
				break;
		}

		JavaRDD<Integer> results = clusters.predict(dataset);
		results.saveAsTextFile(OUTPUT);

		Map<Integer, Long> how_many = results.countByValue();
		for (Map.Entry<Integer, Long> e : how_many.entrySet())
			System.out.println("Cluster " + e.getKey() + ":\t" + e.getValue());

		ss.stop();
	}
}
