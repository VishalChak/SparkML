package textAnalytica;

import java.util.Arrays;
import java.util.List;

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
		
		sentenceData.show();
		session.stop();
	}
}
