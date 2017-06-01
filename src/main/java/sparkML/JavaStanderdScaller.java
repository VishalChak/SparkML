package sparkML;

import org.apache.commons.beanutils.ConvertUtils;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.ml.feature.StandardScaler;
import org.apache.spark.ml.feature.StandardScalerModel;
import org.apache.spark.ml.linalg.VectorUDT;
import org.apache.spark.ml.linalg.Vectors;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

public class JavaStanderdScaller {
	public static void main(String[] args) {
		SparkSession spark = SparkSession.builder().appName("Scaler").master("local").getOrCreate();

		System.setProperty("hadoop.home.dir", "D:/Vishal/hadoopWinUtill/");

		StructType schema = new StructType(
				new StructField[] { new StructField("features", new VectorUDT(), false, Metadata.empty()), });
		String titanic_ph = "D:/Vishal/Kaggle/Titanic/test.csv";
		JavaRDD<String> rdd = spark.read().option("header", true).textFile(titanic_ph).toJavaRDD();
		Dataset<Row> set = spark.read().option("header", true).csv(titanic_ph);
		set.printSchema();
		JavaRDD<Row> rdd1 = rdd.map(new Function<String, Row>() {
			private static final long serialVersionUID = 1L;

			public Row call(String arg0) throws Exception {
				String[] arr = arg0.split(",");
				double[] doubleValues = (double[]) ConvertUtils.convert(arr, Double.TYPE);
				return RowFactory.create(Vectors.dense(doubleValues));
			}
		});
		Dataset<Row> dataset = spark.createDataFrame(rdd1, schema);
//		dataset.printSchema();
		StandardScaler scaler = new StandardScaler().setInputCol("features").setOutputCol("scaledFeatures")
				.setWithStd(true).setWithMean(false);
		StandardScalerModel scalerModel = scaler.fit(dataset);
		Dataset<Row> scaledData = scalerModel.transform(dataset);	
	}
}
