package sparkML;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.commons.beanutils.ConvertUtils;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.ml.feature.PCA;
import org.apache.spark.ml.feature.PCAModel;
import org.apache.spark.ml.linalg.VectorUDT;
import org.apache.spark.ml.linalg.Vectors;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

public class JavaPCAExample {
	public static void main(String[] args) {

		SparkSession spark = SparkSession.builder().master("local").appName("JavaPCAExample").getOrCreate();
		System.setProperty("hadoop.home.dir", "D:/Vishal/hadoopWinUtill/");

		StructType schema = new StructType(
				new StructField[] { new StructField("features", new VectorUDT(), false, Metadata.empty()), });
		String titanic_ph = "D:/Vishal/Kaggle/Titanic/test.csv";
		JavaRDD<String> rdd = spark.read().textFile(titanic_ph).toJavaRDD();
		
		JavaRDD<Row> rdd1 = rdd.map(new Function<String, Row>() {

			public Row call(String arg0) throws Exception {
				String[] arr = arg0.split(",");
				double[] doubleValues = (double[]) ConvertUtils.convert(arr, Double.TYPE);
				System.out.println(doubleValues.length);
				double[] doubleVa = new double [doubleValues.length+1];
				System.out.println(doubleVa.length);
				for(int i=0 ;i < doubleValues.length; i++){
					doubleVa[i] = doubleValues[i];
				}
				doubleVa[doubleValues.length+1] = 50.0; 
				return RowFactory.create(Vectors.dense(doubleVa));
			}
		});
		
		rdd1.count();
		Dataset<Row> dataset = spark.createDataFrame(rdd1, schema);
		dataset.show();
		spark.stop();
	}
}