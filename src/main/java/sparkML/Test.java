package sparkML;


import java.util.Arrays;
import java.util.List;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;

public class Test {
	public static void main(String args[]){
		SparkSession session = SparkSession.builder().master("local").appName("Test").getOrCreate();
		List<String> data = Arrays.asList("Vishal","Babu", "Chak");
		
//		 List<Row>data = Arrays.asList(
//				 RowFactory.create(Vectors.dense(1,2,3,4,5)),
//				 RowFactory.create(Vectors.dense(5,4,3,2,1))
//				 );
		 Dataset<String> ds = session.createDataset(data,Encoders.STRING());
		 ds.show();
	}
}
