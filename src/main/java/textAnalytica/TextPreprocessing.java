package textAnalytica;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.types.DataTypes;

public class TextPreprocessing {
	public static void main(String[] args) {
		SparkSession session = SparkSession.builder().appName("EndTOEnd").master("local").getOrCreate();
		String path = "D:\\Vishal\\DataSets\\incident_new.csv";
		Dataset<Row> dataset = session.read().option("header", true).option("inferschema", true).csv(path);
		
		dataset.createOrReplaceTempView("table");
		session.udf().register("toLower", new UDF1<String, String>() {

			public String call(String t1) throws Exception {
				if (null != t1){
					return t1.toLowerCase();
				} else return "";
			}
			
		}, DataTypes.StringType);
		
		session.sql("select *, toLower(description) AS new_col from table").show();
		session.stop();
	}

	/*private void toLowe(SparkSession session, Dataset<Row> df) {
		df.createOrReplaceTempView("citytemps");
		// Register the UDF with our SQLContext
		session.udf().register("CTOF", new UDF1<Double, Double>() {
			@Override
			public Double call(Double degreesCelcius) {
				return ((degreesCelcius * 9.0 / 5.0) + 32.0);
			}
		}, DataTypes.DoubleType);
		session.sql("SELECT city, CTOF(avgLow) AS avgLowF, CTOF(avgHigh) AS avgHighF FROM citytemps").show();
	}*/
}