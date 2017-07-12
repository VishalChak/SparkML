package Utility;

import java.util.ArrayList;
import java.util.HashMap;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.stat.MultivariateStatisticalSummary;
import org.apache.spark.mllib.stat.Statistics;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class StatisticalSummary {
	public static void main(String[] args) {
		
		SparkSession session = SparkSession.builder().appName("Stastical Summary").master("local").getOrCreate();
		String path = "D:\\Vishal\\DataSets\\dataset_diabetes\\diabetic_data.csv";
		Dataset<Row> dataset = session.read().option("HeaDer", true).option("inferschema", true).csv(path);
		
		
//		Given function describe summary for all numeric columns....   
		dataset.describe().show();
		
		String[] columns = dataset.describe().columns();
		for(int  i = 0 ; i<columns.length;i++){
			System.out.println(columns[i]);
		}
		
		
//		describe only specific variable 
		dataset.describe("patient_nbr","admission_type_id").show();
		
//		Covariance B/W to variable
//		Covariance is a measure of how two variables change with respect to each other
		
		double corr = dataset.stat().corr("num_lab_procedures", "num_procedures");
		System.out.println(corr);
		
//		Cross Tabulation provides a table of the frequency distribution for a set of variables. 
		dataset.stat().crosstab("num_lab_procedures", "num_procedures").show();
		
		
		

		
		session.stop();
		
		
	
		
	}
	
}
