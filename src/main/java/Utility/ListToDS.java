package Utility;


import java.util.Arrays;
import java.util.List;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;

public class ListToDS {
	public static void main(String args[]){
		SparkSession session = SparkSession.builder().master("local[*]").appName("Test").getOrCreate();
		List<String> data = Arrays.asList("Vishal","Babu", "Chak");
		
		Dataset<String> ds = session.createDataset(data,Encoders.STRING());
		long count = ds.count();
		System.out.println(count);
		ds.show();
		session.stop();
	}
}
