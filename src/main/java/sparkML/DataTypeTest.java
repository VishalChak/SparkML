package sparkML;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.ml.feature.LabeledPoint;
import org.apache.spark.ml.linalg.Vectors;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class DataTypeTest {
	public static void convert(int x) {
		SparkSession session = SparkSession.builder().master("local").getOrCreate();
		System.setProperty("hadoop.home.dir", "D:/Vishal/hadoopWinUtill/");
		String path = "D:/Vishal/Kaggle/Titanic/test.csv";
		Dataset<Row> dataset = session.read().csv(path);
		JavaRDD<Row> rdd =dataset.toJavaRDD();
		final int labelInx = x;
		
		JavaRDD<LabeledPoint> lpRdd = rdd.map(new Function<Row, LabeledPoint>() {
			public LabeledPoint call(Row arg0) throws Exception {
				double [] arr = new double[arg0.size()];
				for(int i=0;i<arg0.length();i++){
						arr[i] = Double.parseDouble(arg0.get(i).toString()); 
				}
				LabeledPoint lp = new LabeledPoint( Double.parseDouble(arg0.get(labelInx).toString()),Vectors.dense(arr));
				return lp;
			}
		});
		lpRdd.foreach(new VoidFunction<LabeledPoint>() {
			public void call(LabeledPoint arg0) throws Exception {
				System.out.println(arg0);
				
			}
		});
		session.stop();
	}
	
	public static void main(String[] args) {
		DataTypeTest.convert(4);
	}
}
