package sparkML;


import java.util.Arrays;
import java.util.List;

import org.apache.spark.ml.linalg.VectorUDT;
import org.apache.spark.ml.linalg.Vectors;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

public class ListToDS_DF {
	public static void main(String args[]){
		SparkSession session = SparkSession.builder().master("local[*]").appName("Test").getOrCreate();
		
		 List<Row>data = Arrays.asList(
				 RowFactory.create(Vectors.dense(1,2,3)),
				 RowFactory.create(Vectors.dense(5,4,3,2))
				 );
		 
		 StructType schema = new StructType(new StructField[]{
			      new StructField("features", new VectorUDT(), false, Metadata.empty()),
			    });
		 
		 Dataset<Row> ds = session.createDataFrame(data, schema);
		 long count = ds.count();
		 System.out.println(count);
		 ds.show();
		 session.stop();
	}
}
