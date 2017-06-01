package sparkSQL;


import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.spark.ml.linalg.VectorUDT;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import scala.collection.Iterator;
import sparkML.DataTypeValidator;
import sparkML.SelectCol;

public class EncoderTest {
	public static void main(String[] args) {
		SparkSession session = SparkSession.builder().master("local").appName("Encoder").getOrCreate();
		System.setProperty("hadoop.home.dir", "D:/Vishal/hadoopWinUtill/");
		
		String titanic_ph = "D:/Vishal/Kaggle/Titanic/test.csv";
		Dataset<Row> set = session.read().csv(titanic_ph);
//		.option("header", true)
		ArrayList<String> list = new ArrayList<String>();
		list.add("Age");
		
		Dataset<Row> newData = SelectCol.getcol(set, list);
		StructType sc = newData.schema();
		System.out.println(DataTypeValidator.validate(newData));
	}
}
