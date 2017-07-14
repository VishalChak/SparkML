package ml.classification;



import java.util.ArrayList;
import java.util.HashMap;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import ml.utility.SparkMLUtility;



public class logisticRegression {
	public static void main(String[] args) {
		SparkSession session = SparkSession.builder().master("local").appName("K_MEANS").getOrCreate();
		String log_Path = "D:\\Vishal\\DataSets\\harman_test.csv";
		String path_titanic = "D:\\Vishal\\Kaggle\\Titanic\\train.csv";
		String path = "D:\\Vishal\\DataSets\\dataset_diabetes\\diabetic_data.csv";
		
		Dataset<Row> dataset = session.read().option("HeaDer", true).option("inferschema", true).csv(path_titanic);
		
		SparkMLUtility.setSession(session);
		HashMap<String, ArrayList<String>> map = SparkMLUtility.divideSchema(dataset.schema());
		
		ArrayList<String> numColumns = map.get("numColumns");
		
		Dataset<Row>numDataSet = SparkMLUtility.selectColumns(dataset, numColumns);
		
		numDataSet.show();
		
		String lable = numColumns.get(numColumns.size()-1);
		numColumns.remove(numColumns.size()-1);
		Dataset<Row> pointDataSet = SparkMLUtility.createLabledPointDataSet(numDataSet, lable, numColumns);
//		pointDataSet.show();
		
		/*LogisticRegression lr = new LogisticRegression()
				  .setMaxIter(10)
				  .setRegParam(0.3)
				  .setElasticNetParam(0.8);
		
		LogisticRegressionModel lrModel = lr.fit(pointDataSet);
		
		System.out.println("Coefficients: "
				  + lrModel.coefficients() + " Intercept: " + lrModel.intercept());*/
		
		session.stop();
	}
}
