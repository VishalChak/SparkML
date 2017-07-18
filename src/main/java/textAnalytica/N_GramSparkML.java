package textAnalytica;

import java.util.Arrays;
import java.util.List;

import org.apache.spark.ml.feature.NGram;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

public class N_GramSparkML {
	public static void main(String[] args) {
		SparkSession session = SparkSession.builder().master("local").appName("n-gram").getOrCreate();
		
		List<Row> data = Arrays.asList(
				  RowFactory.create(0.0, Arrays.asList("Hi", "I", "heard", "about", "Spark")),
				  RowFactory.create(1.0, Arrays.asList("I", "wish", "Java", "could", "use", "case", "classes")),
				  RowFactory.create(2.0, Arrays.asList("Logistic", "regression", "models", "are", "neat"))
				);

		StructType schema = new StructType(new StructField[]{
		  new StructField("label", DataTypes.DoubleType, false, Metadata.empty()),
		  new StructField(
		    "words", DataTypes.createArrayType(DataTypes.StringType), false, Metadata.empty())
		});
		
		Dataset<Row> dataset = session.createDataFrame(data, schema);
		
		
		NGram gram = new NGram().setInputCol("words").setOutputCol("ngrams").setN(1);
		
		Dataset<Row> datasetGram = gram.transform(dataset);
		
		for (Row r : datasetGram.select("ngrams", "label").takeAsList(3)) {
			  java.util.List<String> ngrams = r.getList(0);
			  for (String ngram : ngrams)
				  System.out.print(ngram + " --- ");
			  System.out.println();
		}
		session.stop();
	}
}
