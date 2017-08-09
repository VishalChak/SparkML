package textAnalytica;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.ml.feature.RegexTokenizer;
import org.apache.spark.ml.feature.VectorAssembler;
import org.apache.spark.ml.feature.Word2Vec;
import org.apache.spark.ml.feature.Word2VecModel;
import org.apache.spark.ml.linalg.DenseVector;
import org.apache.spark.ml.linalg.SparseVector;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

public class EndToEnd {
	public static Dataset<Row> getFinalData(SparkSession session) {
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
		
		
		VectorAssembler assembler = new VectorAssembler().setInputCols(new String[]{"vec1", "vec2"}).setOutputCol("feature");
		Dataset<Row> transform = assembler.transform(dataset);
		
/*//		transform.show();
		Dataset<Row> transform2 = transform.select("feature");
		
		JavaRDD<Row> javaRDD = transform2.toJavaRDD().map(new Function<Row, Row>() {

			*//**
			 * 
			 *//*
			private static final long serialVersionUID = 1L;

			public Row call(Row arg0) throws Exception {
				DenseVector denseVector = null;
				SparseVector sparseVector = null;
				double arr[];
				if (arg0.getAs(0) instanceof DenseVector){
					denseVector = arg0.getAs(0);
					arr = denseVector.toArray();
				} else {
					sparseVector = arg0.getAs(0);
					arr = sparseVector.toArray();
				}
				for(double d : arr){
					System.out.print( +" ");
				}
//				System.out.println(arr.length);
				return RowFactory.create(arr);
			}
		});
		javaRDD.foreach(new VoidFunction<Row>() {
			
			public void call(Row arg0) throws Exception {
				
			}
		});
		
		StructType schema = new StructType(new StructField[]{
			    new StructField("features", DataTypes.createArrayType(DataTypes.DoubleType), false, Metadata.empty()),
			});
		
		Dataset<Row> dataFrame = session.createDataFrame(javaRDD, schema);
		dataFrame.show();
		
		dataFrame.write().parquet("D:/par_arr");*/
		return transform;
	}
	
	public static void main(String[] args) {
		SparkSession session = SparkSession.builder().appName("ENN").master("local").getOrCreate();
		Dataset<Row> dataset = getFinalData(session);
		dataset.show();
	}

}
