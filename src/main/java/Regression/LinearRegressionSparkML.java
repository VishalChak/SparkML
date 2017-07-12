package Regression;

import java.util.ArrayList;
import java.util.HashMap;

import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator;
import org.apache.spark.ml.regression.LinearRegression;
import org.apache.spark.ml.regression.LinearRegressionModel;
import org.apache.spark.ml.regression.LinearRegressionTrainingSummary;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.json.simple.JSONObject;

import Utility.SparkMLUtility;

public class LinearRegressionSparkML {
	public static void main(String[] args) {
		SparkSession session = SparkSession.builder().master("local").appName("K_MEANS").getOrCreate();
		String path = "D:\\Vishal\\DataSets\\dataset_diabetes\\diabetic_data.csv";
		Dataset<Row> dataset = session.read().option("HeaDer", true).option("inferschema", true).csv(path);
		
		System.setProperty("hadoop.home.dir", "D:\\LnT\\winutils-master\\winutils-master\\hadoop-2.6.0");
		
		
		SparkMLUtility.setSession(session);
		HashMap<String, ArrayList<String>> map = SparkMLUtility.divideSchema(dataset.schema());
		
		ArrayList<String> numColumns = map.get("numColumns");
		
		Dataset<Row>numDataSet = SparkMLUtility.selectColumns(dataset, numColumns);
		String lable = numColumns.get(numColumns.size()-1);
		numColumns.remove(numColumns.size()-1);
		
		Dataset<Row> pointDataSet = SparkMLUtility.createLabledPointDataSet(numDataSet, lable, numColumns);
		pointDataSet.show();
		
		LinearRegression lr = new LinearRegression().setLabelCol("label").setFeaturesCol("features").setMaxIter(10)
				  .setRegParam(0.3)
				  .setElasticNetParam(0.8);
		
		LinearRegressionModel lrModel = lr.fit(pointDataSet);
		
		
		/*try {
			System.out.println(getSummaryOfModel(lrModel));
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}*/
		
		Dataset<Row> predictions = lrModel.transform(pointDataSet);
//		predictions.show();
		
		
		MulticlassClassificationEvaluator evaluator = new MulticlassClassificationEvaluator()
				  .setLabelCol("label")
				  .setPredictionCol("prediction");
				
		
		Double accuracy = evaluator.evaluate(predictions);
		System.out.println(accuracy);
		
		
		session.stop();
	}
	
	@SuppressWarnings("unchecked")
	public static String getSummaryOfModel(LinearRegressionModel model) throws Exception{
		JSONObject modelSummary = new JSONObject();
		LinearRegressionTrainingSummary summary = model.summary();
		modelSummary.put("Mean squared error", summary.meanSquaredError());
		modelSummary.put("Root Mean squared error", summary.rootMeanSquaredError());
		modelSummary.put("Mean Absolute error", summary.meanAbsoluteError());
		modelSummary.put("Explained Variance", summary.explainedVariance());
		modelSummary.put("R squared", summary.r2());
		
		return modelSummary.toString();
	}
}
