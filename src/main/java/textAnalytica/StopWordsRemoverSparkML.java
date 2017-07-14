package textAnalytica;

import java.util.Arrays;
import java.util.List;

import org.apache.spark.ml.feature.StopWordsRemover;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

public class StopWordsRemoverSparkML {
	public static void main(String[] args) {
		SparkSession session = SparkSession.builder().appName("StopWordsRemover").master("local").getOrCreate();
		
		
		List<Row> data = Arrays.asList(
				  RowFactory.create(Arrays.asList("I", "saw", "the", "red", "baloon")),
				  RowFactory.create(Arrays.asList("Mary", "had", "a", "little", "lamb"))
				);

		StructType schema = new StructType(new StructField[]{
		  new StructField(
		    "raw", DataTypes.createArrayType(DataTypes.StringType), false, Metadata.empty())
		});
		
		Dataset<Row> dataset = session.createDataFrame(data, schema);
//		dataset.show();
		
		StopWordsRemover remover = new StopWordsRemover().setInputCol("raw").setOutputCol("filtered");
		remover.loadDefaultStopWords("english");
		Dataset<Row> datasetFiltered = remover.transform(dataset);
		
		datasetFiltered.show();
		
		session.stop();
	}
}
