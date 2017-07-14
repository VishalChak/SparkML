package ml.featureExtractors;



import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import ml.utility.SparkMLUtility;



public class Test {
	public static void main(String[] args) {
		SparkSession session = SparkSession.builder().master("local").appName("QuirtileDiscretizer").getOrCreate();
		SparkMLUtility.setSession(session);
		String path = "D:\\Vishal\\DataSets\\dataset_diabetes\\diabetic_data.csv";
		Dataset<Row> dataset = session.read().option("header", true).option("inferschema", true).csv(path);
		
		session.stop();
	}
}
