package textAnalytica;

import java.util.Arrays;
import java.util.List;

import org.apache.spark.ml.feature.Word2Vec;
import org.apache.spark.ml.feature.Word2VecModel;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.ArrayType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

public class Word2VecSparkML {
	public static void main(String[] args) {
		SparkSession session =  SparkSession.builder().appName("Word2Vec").master("local").getOrCreate();
		List<Row> data = Arrays.asList(
				  RowFactory.create(Arrays.asList("Hi I heard about Spark".split(" "))),
				  RowFactory.create(Arrays.asList("I wish Java could use case classes".split(" "))),
				  RowFactory.create(Arrays.asList("Logistic regression models are neat".split(" ")))
				);
		StructType schema = new StructType(new StructField[]{
		  new StructField("text", new ArrayType(DataTypes.StringType, true), false, Metadata.empty())
		});
		Dataset<Row> documentDF = session.createDataFrame(data, schema);
		Word2Vec word2Vec = new Word2Vec().setInputCol("text").setOutputCol("result").setVectorSize(3).setMinCount(0);
		Word2VecModel word2VecModel = word2Vec.fit(documentDF);
		
		Dataset<Row> datasetRes = word2VecModel.transform(documentDF);
		datasetRes.show();
		datasetRes.printSchema();;
		session.stop();
	}
}
