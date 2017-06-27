
package Regression;

import org.apache.spark.sql.SparkSession;

public class LogisticRegressionSummaryExample{
	public static void main(String[] args) {
		SparkSession session = SparkSession.builder().appName("Summary").master("local").getOrCreate();
		
		session.stop();
	}
}