package Clustering;

import java.util.ArrayList;
import java.util.HashMap;

import org.apache.spark.ml.clustering.KMeans;
import org.apache.spark.ml.clustering.KMeansModel;
import org.apache.spark.ml.clustering.KMeansSummary;
import org.apache.spark.ml.linalg.Vector;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.json.simple.JSONObject;

import Utility.SparkMLUtility;

public class K_Means {
	public static void main(String[] args) {
		SparkSession session = SparkSession.builder().master("local").appName("K_MEANS").getOrCreate();
		String path = "D:\\Vishal\\DataSets\\dataset_diabetes\\diabetic_data.csv";
		Dataset<Row> dataset = session.read().option("HeaDer", true).option("inferschema", true).csv(path);
		
		
		SparkMLUtility.setSession(session);
		HashMap<String, ArrayList<String>> map = SparkMLUtility.divideSchema(dataset.schema());
		
		ArrayList<String> numColumns = map.get("numColumns");
		
		Dataset<Row>numDataSet = SparkMLUtility.selectColumns(dataset, numColumns);
		String lable = numColumns.get(numColumns.size()-1);
		numColumns.remove(numColumns.size()-1);
		Dataset<Row> pointDataSet = SparkMLUtility.createLabledPointDataSet(numDataSet, lable, numColumns);
		
		KMeans kmeans = new KMeans().setK(3).setSeed(1).setInitSteps(10);
		KMeansModel model = kmeans.fit(pointDataSet);
		Double computeCost = model.computeCost(pointDataSet);
		
		System.out.println("Within Set Sum of Squared Errors : "+computeCost);
		
		session.stop();
		
		
	}
	
	@SuppressWarnings("unchecked")
	public static String getSummaryOfModel(KMeansModel model) throws Exception{
		JSONObject modelSummary = new JSONObject();
		modelSummary.put("Number of cluster", model.summary().k());
		
		Vector[] clusterCenters = model.clusterCenters();
		long[] clusterSizes = model.summary().clusterSizes();
		
		for(int i=0;i<clusterSizes.length;i++){
			modelSummary.put("Cluster "+(i+1), " Cluster size : "+ clusterSizes[i] +" Cluster center : "+clusterCenters[i]);
		}
		return modelSummary.toString();
	}
}
