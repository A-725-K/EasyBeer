package spark;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.time.Instant;
import java.time.Duration;

import org.apache.spark.sql.SparkSession;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.mllib.clustering.BisectingKMeans;
import org.apache.spark.mllib.clustering.BisectingKMeansModel;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;

public class Bisect_Clustering {
	public static final String DATASET = "hdfs://master:9000/user/user10/input/clustering.csv";
	public static final String OUTPUT = "res_bkmeans";

	//Clustering with Bisect K-Means algorithm
	public static void  main(String[] args){
		SparkSession ss = SparkSession
                 .builder()
                 .appName("Bisect")
                 .getOrCreate();

         JavaRDD<String> lines = ss.read().textFile(DATASET).javaRDD();
         JavaRDD<Vector> dataset = lines.map( s -> {
             String[] fields = s.split(",");
             int len = fields.length;
             double[] vals = new double[len];
             for (int i = 0; i < len; i++)
                 vals[i] = Double.parseDouble(fields[i]);
             return Vectors.dense(vals);
         });
         dataset.cache();

		int number_of_clusters = 5;
		BisectingKMeans bkm = new BisectingKMeans().setK(number_of_clusters);
    
	    Instant start = Instant.now();
		BisectingKMeansModel model = bkm.run(dataset);
		Instant stop = Instant.now();

   		System.out.println("Compute Cost: " + model.computeCost(dataset));

	    Vector[] clusterCenters = model.clusterCenters();
    	for (int i = 0; i < clusterCenters.length; i++) {
 			Vector clusterCenter = clusterCenters[i];
	    	System.out.println("Cluster Center " + i + ": " + clusterCenter);
		}

		long timeElapsed = Duration.between(start, stop).toMillis();
		System.out.println("Time in millis:\t" + timeElapsed);
		System.out.println("Time in seconds:\t" + timeElapsed/1000);

        int i = 0;
        int min = 150;
        int max = 250;
        for (Vector v : dataset.collect()) {
            if (i++ > min && i < max)
                System.out.println("Point:\t" + v + "\t\t\t\t belongs to cluster " + model.predict(v));
            else if (i > max)
                break;
        }

        JavaRDD<Integer> results = model.predict(dataset);
        results.saveAsTextFile(OUTPUT);

        Map<Integer, Long> how_many = results.countByValue();
        for (Map.Entry<Integer, Long> e : how_many.entrySet())
             System.out.println("Cluster " + e.getKey() + ":\t" + e.getValue());

		ss.stop();
	}
}
