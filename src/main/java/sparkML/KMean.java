package sparkML;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;
import org.apache.spark.sql.SparkSession;

import org.apache.spark.mllib.clustering.KMeans;
import org.apache.spark.mllib.clustering.KMeansModel;

public class KMean {
	public static void main(String[] args) {
		SparkSession session = SparkSession.builder().master("local").appName("K-Mean").getOrCreate();
		System.setProperty("hadoop.home.dir", "D:/Vishal/hadoopWinUtill/");
		String path = "D:/Vishal/Kaggle/Titanic/test.csv";
		
		JavaRDD<String> data = session.read().textFile(path).toJavaRDD();
		JavaRDD<Vector> parsedData = data.map(
				  new Function<String, Vector>() {
				    public Vector call(String s) {
				      String[] sarray = s.split(",");
				      double[] values = new double[sarray.length];
				      for (int i = 0; i < sarray.length; i++) {
				        values[i] = Double.parseDouble(sarray[i]);
				      }
				      return Vectors.dense(values);
				    }
				  }
				);
		parsedData.cache();
		
		int numClusters = 2;
		int numIterations = 20;
		KMeansModel clusters = KMeans.train(parsedData.rdd(), numClusters, numIterations);
		System.out.println("Cluster centers:");
		for (Vector center: clusters.clusterCenters()) {
		  System.out.println(" " + center);
		}
		double cost = clusters.computeCost(parsedData.rdd());
		System.out.println("Cost: " + cost);
		session.stop();
	}
}
