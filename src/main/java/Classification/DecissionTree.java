package Classification;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.ForeachFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.ml.linalg.Vectors;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import Utility.SparkMLLib;
import scala.Function1;
import scala.collection.Iterator;
import scala.runtime.BoxedUnit;

public class DecissionTree {
	public static void main(String[] args) {
		SparkSession  session = SparkSession.builder().appName("DT").master("local").getOrCreate();
		String path  = "D:\\Vishal\\DataSets\\dataset_diabetes\\diabetic_data.csv";
		Dataset<Row> dataset = session.read().option("inferschema", true).option("header", true).csv(path);
		
		HashMap<String, ArrayList<String>> hashMap = divideSchema(dataset.schema());
		
//		System.out.println(hashMap.get("numColumns").size());
//		System.out.println(hashMap.get("otherColumns").size());
		
		String[] columns = dataset.columns();
		ArrayList columnsList = new ArrayList(Arrays.asList(columns));
		
		ArrayList<String> featureList = new ArrayList<String>();
		featureList.add("admission_source_id");
		featureList.add("time_in_hospital");
		featureList.add("num_lab_procedures");
		featureList.add("num_procedures");
		featureList.add("num_medications");
		
		String lebel = "patient_nbr";
		
		session.stop();
		
	}
	
	public static HashMap<String, ArrayList<String>> divideSchema(StructType schema) {
		
		Iterator<StructField> iterator = schema.iterator();
		ArrayList<String> numColumns = new ArrayList<String>();
		ArrayList<String> otherColumns = new ArrayList<String>();
		
		ArrayList<DataType> numericDatatypes = new ArrayList<DataType>();
		numericDatatypes.add(DataTypes.DoubleType);
		numericDatatypes.add(DataTypes.FloatType);
		numericDatatypes.add(DataTypes.IntegerType);
		numericDatatypes.add(DataTypes.LongType);
		numericDatatypes.add(DataTypes.ShortType);
		
		while (iterator.hasNext()){
			StructField field = iterator.next();
			if (numericDatatypes.contains(field.dataType())){
				numColumns.add(field.name());
			} else {
				otherColumns.add(field.name());
			}
		}
		HashMap<String, ArrayList<String>> map = new HashMap<String, ArrayList<String>>();
		map.put("numColumns", numColumns);
		map.put("otherColumns", otherColumns);
		return map ;
	}
	
	
	public static Dataset<Row> createLabledPointDataSet(SparkSession session ,Dataset<Row> dataset, String targetCol,
			ArrayList<String> featureColumns) {
		
		final int len = featureColumns.size();
		final int  labeledIndex = 0;
		JavaRDD<Row> rdd = dataset.toJavaRDD().map(new Function<Row, Row>() {
			public Row call(Row arg0) throws Exception {
				int j = 0;
				double lable = 0.0;
				double[] arr = new double[len];
				for (int i = 0; i < arg0.length(); i++) {
					double val = 0.0;
					if (null != arg0.get(i)) {
						val = Double.parseDouble(arg0.get(i).toString());
					}
					if (i == labeledIndex) {
						lable = val;
					} else {
						arr[j++] = val;
					}
				}
				return RowFactory.create(lable, Vectors.dense(arr));
			}
		});
		return session.createDataFrame(rdd, SparkMLLib.getLabeledPointSchema());
	}
}
