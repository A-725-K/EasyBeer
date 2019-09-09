package spark;

import scala.Tuple2;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaPairRDD;

import org.apache.spark.sql.SparkSession;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.mllib.regression.LabeledPoint;
import org.apache.spark.mllib.tree.DecisionTree;
import org.apache.spark.mllib.tree.model.DecisionTreeModel;
import org.apache.spark.mllib.util.MLUtils;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;

import java.util.Map;
import java.util.HashMap;
import java.util.regex.Pattern;

import java.text.DecimalFormat;

public class ML_DecisionTree {
	public static final Pattern COMMA = Pattern.compile(",");
	public static final String DATASET = "hdfs://master:9000/user/user10/input/ml_data.csv";

	//Cross Validation results
	private static class X_Validation {
		public double accuracy;
		public double test_err;
		public int max_depth;
		public int max_bins;
		public int test_num;
		public String impurity;
		public DecisionTreeModel model;

		public X_Validation(double acc, double err, DecisionTreeModel m, int md, int mb, String imp, int n) {
			this.accuracy = acc;
			this.test_err = err;
			this.model = m;
			this.max_depth = md;
			this.max_bins = mb;
			this.impurity = imp;
			this.test_num = n;
		}

		public void prettyPrint() {
			DecimalFormat df = new DecimalFormat("###.##");
			System.out.println("Test ( " + (this.test_num + 1) + " )");
			System.out.println("\tHyperparameters:");
			System.out.println("\t|-- Impurity:\t" + this.impurity);
			System.out.println("\t|-- Max depth:\t" + this.max_depth);
			System.out.println("\t|-- Max bins:\t" + this.max_bins);
			System.out.println("\n\tResults:");
			System.out.println("\t|==> Accuracy:\t" + df.format(this.accuracy) + " %");	
			System.out.println("\t|==> Test err:\t" + df.format(this.test_err) + " %");
			System.out.println("________________________________________\n");
		}
	}

	//Try to understand a model to predict the style of a new beer with decision tree
	public static void main(String[] args) {
		SparkSession ss = SparkSession
				.builder()
				.appName("Decision Tree")
				.getOrCreate();
		
		JavaRDD<String> lines = ss.read().textFile(DATASET).javaRDD();
		JavaPairRDD<Integer, Vector> dataset = lines.mapToPair(s -> {
			String[] fields = COMMA.split(s);
			int len = fields.length-1;
			double[] vals = new double[len];
			/*double[] vals = new double[]{Double.parseDouble(fields[3]),
										 Double.parseDouble(fields[4]),
										 Double.parseDouble(fields[5]),
										 Double.parseDouble(fields[9])};*/
			for (int i = 0; i < len; i++)
				vals[i] = Double.parseDouble(fields[i]);
			int label = Integer.parseInt(fields[len]);
			return new Tuple2<>(label, Vectors.dense(vals));
		});
		dataset.cache();

		int num_classes = 148;
		Map<Integer, Double> fractions = new HashMap<>();
		//long seed = 42l;
		for (int i = 0; i < num_classes; i++)
			fractions.put(i, 0.65);	//65% training, 35% test

		//split dataset in training and test sets
		JavaPairRDD<Integer, Vector> training_set = dataset.sampleByKey(false, fractions);
		JavaPairRDD<Integer, Vector> test_set = dataset.subtract(training_set);
		JavaRDD<LabeledPoint> tr_set = training_set.map(p -> new LabeledPoint(Double.valueOf(p._1()), p._2()));
		JavaRDD<LabeledPoint> ts_set = test_set.map(p -> new LabeledPoint(Double.valueOf(p._1()), p._2()));
		tr_set.cache();
		ts_set.cache();

		/*System.out.println("#################### |DS| = " + dataset.count());	
		System.out.println("#################### |TR| = " + training_set.count());
		System.out.println("#################### |TS| = " + test_set.count());*/

		//hyperparameters of the algorithm
		Map<Integer, Integer> categoricalFeaturesInfo = new HashMap<>();
		String[] impurities = new String[]{"gini", "entropy"};
		int[] max_depths = new int[]{3, 5, 7, 10, 12};
		int[] max_bins = new int[]{16, 32, 64, 128, 256};

		//how much computation we want to do
		int num_tests = impurities.length * max_depths.length * max_bins.length;
	
		//************************	
		//*** CROSS VALIDATION ***
		//************************	
		X_Validation[] xvs = new X_Validation[num_tests];
		int idx = 0;
		for (String im : impurities) {
			for (int md : max_depths) {
				for (int mb : max_bins) {		
					DecisionTreeModel model = DecisionTree
							.trainClassifier(tr_set, num_classes, categoricalFeaturesInfo, im, md, mb);
					JavaPairRDD<Double, Double> res_pred = ts_set
							.mapToPair(p -> new Tuple2<>(model.predict(p.features()), p.label()));
					double test_err = res_pred
							.filter(pl -> !pl._1().equals(pl._2())).count() / (double)ts_set.count();
					xvs[idx] = new X_Validation((1-test_err)*100, test_err*100, model, md, mb, im, idx++);
				}
			}
		}

		//print results	
		for (X_Validation xv : xvs)
			xv.prettyPrint();
		
		//get best model	
		X_Validation best_model = xvs[0];
		for (int i = 1; i < xvs.length; i++)
			if (xvs[i].accuracy > best_model.accuracy)
				best_model = xvs[i];		

		System.out.println("The best results were obtained with:");
		best_model.prettyPrint();

		ss.stop();
	}
}
