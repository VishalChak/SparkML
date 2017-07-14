package textAnalytica;

import java.util.Arrays;
import java.util.List;

import org.apache.spark.ml.feature.CountVectorizer;
import org.apache.spark.ml.feature.CountVectorizerModel;
import org.apache.spark.ml.linalg.Vector;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.ArrayType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

public class CountVectorizerSparkML {
	public static void main(String[] args) {
		SparkSession session = SparkSession.builder().master("local").appName("CountVectorizer").getOrCreate();
		
		List<Row> data = Arrays.asList(
				  RowFactory.create(Arrays.asList("a", "b", "c")),
				  RowFactory.create(Arrays.asList("a", "b", "b", "c", "a"))
		);
		StructType schema = new StructType(new StructField [] {
				  new StructField("text", new ArrayType(DataTypes.StringType, true), false, Metadata.empty())
				});
		
		
		Dataset<Row> df = session.createDataFrame(data, schema);
//		df.show();
		
		CountVectorizer countVectorizer = new CountVectorizer().setInputCol("text").setOutputCol("feature").setVocabSize(3).setMinDF(2);
		CountVectorizerModel countVectorizerModel = countVectorizer.fit(df);
		Dataset<Row> res_df = countVectorizerModel.transform(df);
		
		for(Row r: res_df.select("feature").takeAsList(5)){
			Vector vector = r.getAs(0);
			System.out.println(vector);
		}
		
		
		session.stop();
	}
}
