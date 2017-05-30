package sparkML;


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
    
    StructType schema = new StructType(new StructField[]{
      new StructField("features", new VectorUDT(), false, Metadata.empty()),
    });
	  String titanic_ph = "D:/Vishal/Kaggle_Competition/Titanic/test.csv";
	  JavaRDD<String> rdd = spark.read().textFile(titanic_ph).toJavaRDD();
	  
	  JavaRDD<Row> rdd1 = rdd.map(new Function<String, Row>() {

		public Row call(String arg0) throws Exception {
			String [] arr = arg0.split(",");
			double[] doubleValues = (double[])ConvertUtils.convert(arr, Double.TYPE);
			return RowFactory.create(Vectors.dense(doubleValues));
		}
	});
	 Dataset<Row> dataset= spark.createDataFrame(rdd1, schema);

    PCAModel pca = new PCA()
      .setInputCol("features")
      .setOutputCol("pcaFeatures")
      .setK(1)
      .fit(dataset);

    Dataset<Row> result = pca.transform(dataset).select("pcaFeatures");
    result.show();
    spark.stop();
  }
}