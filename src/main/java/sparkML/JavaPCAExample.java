package sparkML;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.ml.feature.PCA;
import org.apache.spark.ml.feature.PCAModel;
import org.apache.spark.ml.linalg.VectorUDT;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

public class JavaPCAExample {
	public static void main(String args[]) {

		SparkSession spark = SparkSession.builder().master("local").appName("JavaPCAExample").getOrCreate();
		System.setProperty("hadoop.home.dir", "D:/Vishal/hadoopWinUtill/");

		StructType schema = new StructType(
				new StructField[] { new StructField("features", new VectorUDT(), false, Metadata.empty()), });
		String titanic_ph = "D:/Vishal/Kaggle/Titanic/test.csv";
		Dataset<Row> dataSource = spark.read().csv(titanic_ph);
		Dataset<Row> data = dataSource.select("_c3","_c4","_c5");
		Dataset<Row> reData = dataSource.select("_c0","_c1","_c2");
		JavaRDD<Row> rdd = data.toJavaRDD();
		JavaRDD<Row>rdd1 = SparkMLLib.strRddToVectorRdd(rdd);
		Dataset<Row> dataset = spark.createDataFrame(rdd1, schema);
		PCAModel pca = new PCA()
			      .setInputCol("features")
			      .setOutputCol("pcaFeatures")
			      .setK(3)
			      .fit(dataset);
		Dataset<Row> result = pca.transform(dataset).select("pcaFeatures");
		JavaRDD<Row> newRdd= SparkMLLib.cbind(reData.toJavaRDD(),result.toJavaRDD());
		newRdd = SparkMLLib.strRddToVectorRdd(newRdd);
		Dataset<Row> resDataSet = spark.createDataFrame(newRdd, schema);
		resDataSet.show();
		spark.stop();
	}
}