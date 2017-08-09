package textAnalytica;

import org.apache.spark.sql.SparkSession;

public class Hive_test {
	public static void main(String[] args) {
		SparkSession session = SparkSession.builder().appName("Hive").master("local").enableHiveSupport().getOrCreate();
	
	}
}
