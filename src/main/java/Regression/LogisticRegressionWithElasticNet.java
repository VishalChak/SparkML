package Regression;

import org.apache.spark.sql.SparkSession;

public class LogisticRegressionWithElasticNet{
	public static void main(String[] args) {
		SparkSession  session = new SparkSession.Builder().appName("Logistic regression").master("local").getOrCreate();
		
		
	}
}