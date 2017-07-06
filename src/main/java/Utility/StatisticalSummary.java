package Utility;

import java.util.ArrayList;
import java.util.HashMap;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.stat.Statistics;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class StatisticalSummary {
	public static void main(String[] args) {
		
		SparkSession session = SparkSession.builder().appName("Stastical Summary").master("local").getOrCreate();
		String path = "D:\\Vishal\\DataSets\\dataset_diabetes\\diabetic_data.csv";
		Dataset<Row> dataset = session.read().option("HeaDer", true).option("inferschema", true).csv(path);
		
		
		SparkMLUtility.setSession(session);
		HashMap<String, ArrayList<String>> map = SparkMLUtility.divideSchema(dataset.schema());
		
		ArrayList<String> numColumns = map.get("numColumns");
		
		Dataset<Row>numDataSet = SparkMLUtility.selectColumns(dataset, numColumns);
		JavaRDD<Vector> vectorMlLibRdd = SparkMLUtility.getVectorMlLibRdd(numDataSet.toJavaRDD());
		
		
//		Statistics.colStats(numDataSet.toJavaRDD());
		
		
	}
	
}
