package sparkML;


import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;


public class FileToDF_sql {
  public static void main(String[] args) {
	  System.setProperty("hadoop.home.dir", "D:/Vishal/hadoopWinUtill/bin");
	  SparkSession spark = SparkSession.builder().master("local").appName("JavaPCAExample").getOrCreate();
	  String titanic_ph = "D:/Vishal/Kaggle_Competition/Titanic/test.csv";
	  Dataset<Row> df = spark.read().option("header", "true").csv(titanic_ph);
	  df.createOrReplaceTempView("Table");
	  Dataset<Row> sqlDF = spark.sql("SELECT * FROM Table");
	  sqlDF.show();
	  spark.stop();
  }
}