package textAnalytica;


import java.util.Arrays;
import java.util.List;

import org.apache.spark.ml.feature.CountVectorizer;
import org.apache.spark.ml.feature.CountVectorizerModel;
import org.apache.spark.ml.feature.HashingTF;
import org.apache.spark.ml.feature.IDF;
import org.apache.spark.ml.feature.IDFModel;
import org.apache.spark.ml.feature.RegexTokenizer;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

public class test {
	public static void main(String[] args) {
		SparkSession session = SparkSession.builder().appName("TF_IDF").master("local").getOrCreate();
		
		List<Row> data = Arrays.asList(
				  RowFactory.create(0.0, "Hi I heard"),
				  RowFactory.create(0.0, "Hi I heard"),
				  RowFactory.create(1.0, "Hi I heard Hi I heard Hi I heard vishal chak ")
				);
				StructType schema = new StructType(new StructField[]{
				  new StructField("label", DataTypes.DoubleType, false, Metadata.empty()),
				  new StructField("sentence", DataTypes.StringType, false, Metadata.empty())
				});
				
		Dataset<Row> dataset = session.createDataFrame(data, schema);
		
		RegexTokenizer rxtokenizer = new RegexTokenizer().setInputCol("sentence").setOutputCol("words");
		dataset = rxtokenizer.transform(dataset);
		
		CountVectorizer countVectorizer = new CountVectorizer().setInputCol("words").setOutputCol("countVec");
		HashingTF  hashingTF = new HashingTF().setInputCol("words").setOutputCol("hashVec").setNumFeatures(20);
		
		CountVectorizerModel countVectorizerModel = countVectorizer.fit(dataset);
		dataset = countVectorizerModel.transform(dataset);
		
		dataset = hashingTF.transform(dataset);
		
		for(Row r: dataset.select("countVec","hashVec").takeAsList(5)){
			System.out.println(r.getAs(0));
			System.out.println(r.getAs(1));
			System.out.println();
		}
		
		IDF idf = new IDF().setInputCol("countVec").setOutputCol("newFeature");
		IDFModel idfModel = idf.fit(dataset);
		
		dataset = idfModel.transform(dataset);
		
		for(Row r : dataset.select("newFeature").takeAsList(5)){
			System.out.println(r.getAs(0));
		}
		
				
		session.stop();
	}
}
