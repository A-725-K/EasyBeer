package spark;

import scala.Tuple2;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.SparkSession;

import java.util.Map;
import java.util.Arrays;
import java.util.List;
import java.util.regex.Pattern;

public final class Query5 {
	public static final Pattern COMMA = Pattern.compile(",");
	public static final String DATASET = "hdfs://master:9000/user/user10/input/beers.csv";

	private static Tuple2<String, Long> max_brew = new Tuple2<>("", 0l);

     //map:
     //  K  ==>  color
     //  V  ==>  how many beers with that color
     private static void setMax(Map<String, Long> map) {
        map.forEach((k, v) -> {
             if (v >=  max_brew._2())
                 max_brew  = new Tuple2<>(k, v);
         });
     }

	//FOR EACH STYLE SELECT THE GREATEST ABV
	public static void main(String[] args) throws Exception {
		SparkSession ss = SparkSession
				.builder()
				.appName("Q5")
				.getOrCreate();

		JavaRDD<String> lines = ss.read().textFile(DATASET).javaRDD();
		JavaRDD<String> double_malt = lines.filter(b -> !COMMA.split(b)[11].equals("N/A") && Double.valueOf(COMMA.split(b)[11]) > 14.5  &&  Double.valueOf(COMMA.split(b)[6]) > 3.5);
	
		JavaPairRDD<String,String> double_malt_and_names = double_malt.mapToPair(b -> new Tuple2<>(COMMA.split(b)[13],COMMA.split(b)[1]));
		Map<String,Long> count_group_by = double_malt_and_names.countByKey();
	
		setMax(count_group_by);
		
        String maxBrew = max_brew._1();
        JavaPairRDD<String, String> result =  double_malt_and_names.filter(b -> b._1().equals(maxBrew) );
        List<Tuple2<String, String>> list_print  = result.collect();

        System.out.println("The more popular brew method among double malt beers is " + max_brew._2() + ", used in  " + max_brew._1()+ " beers");
		ss.stop();
	}
}
