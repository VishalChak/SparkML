package ml.featureSelectors;



import java.util.ArrayList;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import scala.collection.Iterator;

public class vectorSlice {
	public static void main(String[] args) {
		SparkSession session = SparkSession.builder().appName("VectorSlice").master("local").getOrCreate();
		String path = "D:\\Vishal\\DataSets\\Iris\\Test.csv";
		String loan_path = "D:\\Vishal\\DataSets\\lending-club-loan-data\\loan.csv";
		Dataset<Row> dataset = session.read().option("header", true).option("inferschema", true).csv(loan_path);
		
		System.out.println(dataset.count());
		String[] header = dataset.columns();
		
		dataset.printSchema();
		ArrayList<String> strColms = getStrColms(dataset.schema());
		System.out.println(strColms);
		
		session.stop();
	}
	
	private static ArrayList<String> getStrColms(StructType schema) {
		Iterator<StructField> iterator = schema.iterator();
		ArrayList<String> strColumns = new ArrayList<String>();
		while (iterator.hasNext()){
			StructField field = iterator.next();
			if (field.dataType() == DataTypes.StringType){
				strColumns.add(field.name());
			}
		}
		return strColumns;
	}
	
}
