package textAnalytica;

import java.util.Arrays;
import java.util.List;

import org.apache.spark.ml.feature.HashingTF;
import org.apache.spark.ml.feature.IDF;
import org.apache.spark.ml.feature.IDFModel;
import org.apache.spark.ml.feature.RegexTokenizer;
import org.apache.spark.ml.linalg.Vector;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

public class TF_IDFSparkML {
	public static void main(String[] args) {
		SparkSession session = SparkSession.builder().appName("TF-IDF").master("local").getOrCreate();
		
		List<Row> data = Arrays.asList(
				  RowFactory.create(0.0, "Hi I heard about Spark"),
				  RowFactory.create(0.0, "I wish Java could use case classes"),
				  RowFactory.create(1.0, "Logistic regression models are neat")
				);
		StructType schema = new StructType(new StructField[]{
		  new StructField("label", DataTypes.DoubleType, false, Metadata.empty()),
		  new StructField("sentence", DataTypes.StringType, false, Metadata.empty())
		});
				
		
		Dataset<Row> sentenceData = session.createDataFrame(data, schema);
//		sentenceData.show();
		
		RegexTokenizer regexTokenizer = new RegexTokenizer().setInputCol("sentence").setOutputCol("words").setPattern("\\W");
		Dataset<Row> datasetRex = regexTokenizer.transform(sentenceData);
//		datasetRex.show();
		
		
		int numFeatures = 10;
		HashingTF hashingTF = new HashingTF().setInputCol("words").setOutputCol("rawFeatures").setNumFeatures(numFeatures);
		Dataset<Row> datasetTF = hashingTF.transform(datasetRex);
//		datasetTF.show();
		for(Row r: datasetTF.select("rawFeatures").takeAsList(3)){
			Vector rawFeatures = r.getAs(0);
			System.out.println(rawFeatures);
		}
		
		
		
		IDF idf = new IDF().setInputCol("rawFeatures").setOutputCol("features");
		IDFModel idfModel = idf.fit(datasetTF);
		
		Dataset<Row> datasetIdf = idfModel.transform(datasetTF);
//		datasetIdf.show();
		
//		for (Row r : datasetIdf.select("features", "label").takeAsList(3)){
//			Vector features  = r.getAs(0);
//			Double label = r.getAs(1);
//			System.out.println(features);
//			System.out.println(label);
//		}
		
		session.stop();
	}
}
