package ml.utility;




import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;


public class FileToDF_sql {
  public static void main(String[] args) {
	  System.setProperty("hadoop.home.dir", "D:/Vishal/hadoopWinUtill/bin");
	  SparkSession spark = SparkSession.builder().master("local").appName("JavaPCAExample").getOrCreate();
	  String titanic_ph = "D:/Vishal/Kaggle/Titanic/train.csv";
	  Dataset<Row> df = spark.read().option("header", "true").option("inferSchema","true").csv(titanic_ph);
	  df.createOrReplaceTempView("Table");
	  Dataset<Row> dataset  = removeStringColumns(df);
	  StructType schema = dataset.schema();
	  String[] fieldNames = schema.fieldNames();
	  for(int i = 0 ;i <fieldNames.length;i++){
		  System.out.println(fieldNames[i]);
	  }
	  System.out.println(dataset.head());
	  spark.stop();
  }
  
  
	public static Dataset<Row> removeStringColumns(Dataset<Row> dataset) {
		List<String> dataTypeList = Arrays.asList("DoubleType", "IntegerType", "LongType", "FloatType", "ShortType");
		StructType structType = dataset.schema();
		scala.collection.Iterator<StructField> iterator = structType.iterator();
		while (iterator.hasNext()) {
			StructField structField = iterator.next();
			if (!dataTypeList.contains(structField.dataType().toString())) {
				dataset = dataset.drop(structField.name());
			}
		}
		return dataset;
	}

}