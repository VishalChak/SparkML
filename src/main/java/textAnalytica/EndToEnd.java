package textAnalytica;

import org.apache.spark.ml.feature.RegexTokenizer;
import org.apache.spark.ml.feature.VectorAssembler;
import org.apache.spark.ml.feature.Word2Vec;
import org.apache.spark.ml.feature.Word2VecModel;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class EndToEnd {
	public static void main(String[] args) {
		SparkSession  session = SparkSession.builder().appName("EndTOEnd").master("local").getOrCreate();
		String path = "D:\\Vishal\\DataSets\\incident_new.csv";
		Dataset<Row> dataset = session.read().option("header", true).option("inferschema", true).csv(path);
		
		dataset = dataset.na().drop();
		dataset = dataset.na().fill("");
		
//		dataset.show();
		
		
		RegexTokenizer regexTokenizer1 = new RegexTokenizer().setInputCol("description").setOutputCol("word1").setPattern("\\W");
		RegexTokenizer regexTokenizer2 = new RegexTokenizer().setInputCol("requested_for").setOutputCol("word2").setPattern("\\W");
		
		dataset = regexTokenizer1.transform(dataset);
		dataset = regexTokenizer2.transform(dataset);
		
//		dataset.show();
		
		Word2Vec vec1 = new Word2Vec().setInputCol("word1").setOutputCol("vec1").setVectorSize(3);
		Word2Vec vec2 = new Word2Vec().setInputCol("word2").setOutputCol("vec2").setVectorSize(3);
		
		Word2VecModel vecModel1 = vec1.fit(dataset);
		Word2VecModel vecModel2 = vec2.fit(dataset);
		
		dataset = vecModel1.transform(dataset);
		dataset = vecModel2.transform(dataset);
		
		
		for (Row r : dataset.select("vec1","vec2").takeAsList(5)){
			System.out.println(r.getAs(0)+"..."+r.getAs(1));
		}
		
		VectorAssembler assembler = new VectorAssembler().setInputCols(new String[]{"vec1", "vec2"}).setOutputCol("feature");
		Dataset<Row> transform = assembler.transform(dataset);
		
		for (Row r : transform.select("feature").takeAsList(5)){
			System.out.println(r.getAs(0));
		}
		
		transform.printSchema();
		session.stop();
	}

}
