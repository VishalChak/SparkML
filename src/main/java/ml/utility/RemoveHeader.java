package ml.utility;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class RemoveHeader {
	public static void main(String[] args) {
		SparkSession session = SparkSession.builder().master("local").getOrCreate();
		System.setProperty("hadoop.home.dir", "D:/Vishal/hadoopWinUtill/");
		String path = "D:/Vishal/Kaggle/Titanic/test.csv";
		Dataset<Row> dataset = session.read().csv(path);
		JavaRDD<Row> javaRDD = dataset.toJavaRDD();
		final Row header = dataset.head();
		System.out.println(header);
		JavaRDD<Row> rdd = javaRDD.filter(new Function<Row, Boolean>() {
			public Boolean call(Row arg0) throws Exception {
				if (arg0.equals(header))
					return false;
				else return true;
			}
		});
		System.out.println(javaRDD.count());
		System.out.println(rdd.count());
	}
}
