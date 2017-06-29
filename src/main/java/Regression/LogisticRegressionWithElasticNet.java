package Regression;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class LogisticRegressionWithElasticNet{
	public static void main(String[] args) {
		SparkSession  session = new SparkSession.Builder().appName("Logistic regression").master("local").getOrCreate();
		String path  = "D:\\Vishal\\DataSets\\dataset_diabetes\\diabetic_data.csv";
		Dataset<Row> dataset = session.read().option("inferschema", true).option("header", true).csv(path);
		dataset.show();
		dataset.printSchema();
		
		System.out.println(dataset.count());
		System.out.println(dataset.first().length());
		session.stop();
	}
}