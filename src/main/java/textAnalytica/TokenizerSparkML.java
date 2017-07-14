package textAnalytica;

import java.util.Arrays;
import java.util.List;

import org.apache.spark.ml.feature.RegexTokenizer;
import org.apache.spark.ml.feature.Tokenizer;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

public class TokenizerSparkML {
	@SuppressWarnings("unchecked")
	public static void main(String[] args) {

		SparkSession session = SparkSession.builder().master("local").appName("Tokenizer").getOrCreate();

		List<Row> data = Arrays.asList(RowFactory.create(0, "Hi I heard about Spark"),
				RowFactory.create(1, "I wish Java could use case classes"),
				RowFactory.create(2, "Logistic,regression,models,are,neat"));
		Dataset<Row> sentenceDataFrame = session.createDataFrame(data, schemaText());

		Tokenizer tokenizer = new Tokenizer().setInputCol("sentence").setOutputCol("words");
		Dataset<Row> tokenized = tokenizer.transform(sentenceDataFrame);
		int len = (int) tokenized.count();
		for (Row r : tokenized.select("words", "id").takeAsList(len)) {
			List<String> words = r.getList(0);
			System.out.println(words.size());
			for (String word : words) {
				System.out.print(word + " ");
			}
			System.out.println();
		}

		RegexTokenizer regexTokenizer = new RegexTokenizer().setInputCol("sentence").setOutputCol("words")
				.setPattern("\\W");
		Dataset<Row> rexTokenized = regexTokenizer.transform(sentenceDataFrame);
		
		for (Row r : rexTokenized.select("words", "id").takeAsList(len)) {
			List<String> words = r.getList(0);
			System.out.println(words.size());
			for (String word : words) {
				System.out.print(word + " ");
			}
			System.out.println();
		}
		

		session.stop();

	}

	private static StructType schemaText() {
		return new StructType(new StructField[] { new StructField("id", DataTypes.IntegerType, false, Metadata.empty()),
				new StructField("sentence", DataTypes.StringType, false, Metadata.empty()) });

	}
}
