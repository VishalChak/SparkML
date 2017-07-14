package ml.utility;

import java.util.ArrayList;
import java.util.HashMap;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.ml.linalg.DenseVector;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

public class Test {
	

	public static void main(String[] args) {
		SparkSession session = SparkSession.builder().master("local").appName("K_MEANS").getOrCreate();
		String path = "D:\\Vishal\\DataSets\\dataset_diabetes\\diabetic_data.csv";
		Dataset<Row> dataset = session.read().option("HeaDer", true).option("inferschema", true).csv(path);
		
		
		SparkMLUtility.setSession(session);
		HashMap<String, ArrayList<String>> map = SparkMLUtility.divideSchema(dataset.schema());
		
		ArrayList<String> numColumns = map.get("numColumns");
		
		Dataset<Row>numDataSet = SparkMLUtility.selectColumns(dataset, numColumns);
		
		/*Row first = numDataSet.first();
		for(int i = 0 ; i <first.size() ; i++){
			System.out.println(first.get(i));
		}*/
		String lable = numColumns.get(numColumns.size()-1);
		numColumns.remove(numColumns.size()-1);
		Dataset<Row> pointDataSet = SparkMLUtility.createLabledPointDataSet(numDataSet, lable, numColumns);
		test2(session, pointDataSet);
		
		
		session.stop();
	}
	
	private static StructType doubleSchema(int len) {
		StructField fields [ ] = new StructField[len];
		for(int i = 0 ; i < len; i ++){
			fields[i]= new StructField("Feature"+i, DataTypes.DoubleType, false, Metadata.empty());
		}
		StructType schema = new StructType(fields);
		
		System.out.println(schema.size());
		return schema;
	}
	
	private static void test2(SparkSession session ,Dataset<Row> dataset) {
		Row first = dataset.first();
		int len = 0 ; 
		JavaRDD<Row> rdd = dataset.toJavaRDD().map(new Function<Row, Row>() {
			public Row call(Row arg0) throws Exception {
				
				DenseVector denseVector =  (DenseVector) arg0.get(1);
				double[] values = denseVector.values();
				Double [] arr = new  Double[values.length+1];
				arr[0] = arg0.getDouble(0);
				for(int i = 0; i <values.length;i ++){
					arr[i+1] = values[i];
				}
				return RowFactory.create(arr);
			}
		});
		DenseVector denseVector =  (DenseVector) first.get(1);
		double[] values = denseVector.values();
		StructType doubleSchema = doubleSchema(values.length+1);
		Dataset<Row> dataset2 = session.createDataFrame(rdd, doubleSchema);
		dataset2.show();
		
	}
	
	
}
