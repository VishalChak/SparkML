package Utility;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

import org.apache.commons.beanutils.ConvertUtils;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.ml.linalg.Vector;
import org.apache.spark.ml.linalg.VectorUDT;
import org.apache.spark.ml.linalg.Vectors;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import scala.Tuple2;
import scala.collection.Iterator;

/**
 * Added by Vishal Babu
 */

public class SparkMLLib {

	public static SparkSession session;

	/**
	 * @return the session
	 */
	public static SparkSession getSession() {
		return session;
	}

	/**
	 * @param session
	 *            the session to set
	 */
	public static void setSession(SparkSession session) {
		SparkMLLib.session = session;
	}

	private static List<String> dataTypeList = Arrays.asList("DoubleType", "IntegerType", "LongType", "FloatType",
			"ShortType");
	
	
	
	

	
	public static StructType stringSchema(int size) {
		StructField [] structFields =  new StructField[size];
		for(int i=0; i<structFields.length;i++){
			structFields[i] = new StructField("Field"+i, DataTypes.StringType, false, Metadata.empty());
		}
			
		return new StructType(structFields);
		
	}
	
	
	public static StructType doubleSchema(int size) {
		StructField [] structFields =  new StructField[size];
		for(int i=0; i<structFields.length;i++){
			structFields[i] = new StructField("Field"+i, DataTypes.DoubleType, false, Metadata.empty());
		}
			
		return new StructType(structFields);
		
	}
	
	public static Dataset<Row> getLabeledPointDataSet(Dataset<Row> data, final int labeledIndex) {
		JavaRDD<Row> rdd = data.toJavaRDD().map(new Function<Row, Row>() {

			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			public Row call(Row arg0) throws Exception {
				int j = 0;
				double lable = 0.0;
				double[] arr = new double[arg0.length()];
				for (int i = 0; i < arg0.length(); i++) {
					if (i == labeledIndex) {
						lable = Double.parseDouble(arg0.get(i).toString());
					} else {
						arr[j++] = Double.parseDouble(arg0.get(i).toString());
					}
				}
				return RowFactory.create(lable, Vectors.dense(arr));
			}
		});
		return session.createDataFrame(rdd, getLabeledPointSchema());
	}

	public static Dataset<Row> createLabledPointDataSet1(Dataset<Row> dataset, String targetCol,
			ArrayList<String> featureColumns) {
		final int labeledIndex = getColumnIndex(dataset.schema(), targetCol);
		final int len = featureColumns.size();

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
		return session.createDataFrame(rdd, getLabeledPointSchema());
	}
	
	public static Dataset<Row> createLabledPointDataSet(Dataset<Row> dataset, String targetCol,
			 ArrayList<String> featureColumns) {
		
		ArrayList columnsList = new ArrayList(Arrays.asList(dataset.columns()));
		final ArrayList<Integer> featureIntList = new ArrayList<Integer>(); 
		for(int i = 0 ; i < featureColumns.size(); i++){
			featureIntList.add(columnsList.indexOf(featureColumns.get(i)));
		}
		
		final int labeledIndex = columnsList.indexOf(targetCol);
		
		JavaRDD<Row> rdd = dataset.toJavaRDD().map(new Function<Row, Row>() {
			private static final long serialVersionUID = 1L;

			public Row call(Row arg0) throws Exception {
				String[] arr = new String[featureIntList.size()];
				for(int i = 0 ; i < featureIntList.size();i++){
					arr[i] = arg0.get(featureIntList.get(i))+"";
				}
				double lable = (Double) ConvertUtils.convert(arg0.get(labeledIndex)+"", Double.TYPE);
				double[] doubleValues = (double[]) ConvertUtils.convert(arr, Double.TYPE);
				return RowFactory.create(lable, Vectors.dense(doubleValues));
				
			}
		});
		return session.createDataFrame(rdd, getLabeledPointSchema());
	}
	

	public static int getColumnIndex(StructType schema, String columnName) {
		String[] fieldNames = schema.fieldNames();
		for (int i = 0; i < fieldNames.length; i++) {
			if (fieldNames[i].equalsIgnoreCase(columnName)) {
				return i;
			}
		}
		return fieldNames.length - 1;

	}

	public static ArrayList<String> getFeatureList(StructType schema) {
		String[] fieldNames = schema.fieldNames();
		ArrayList<String> featureList = new ArrayList();
		for (int i = 0; i < fieldNames.length; i++) {
			featureList.add(fieldNames[i]);
		}
		return featureList;

	}

	public static JavaRDD<Vector> getVectorMlRdd(JavaRDD<Row> rdd) {
		return rdd.map(new Function<Row, Vector>() {

			private static final long serialVersionUID = 1L;

			public Vector call(Row row) throws Exception {
				double doubleValues[] = new double[row.length()];
				for (int i = 0; i < row.length(); i++) {
					doubleValues[i] = Double.parseDouble(row.get(i).toString());
				}
				return Vectors.dense(doubleValues);
			}
		});
	}

	public static JavaRDD<org.apache.spark.mllib.linalg.Vector> getVectorMlLibRdd(JavaRDD<Row> rdd) {
		return rdd.map(new Function<Row, org.apache.spark.mllib.linalg.Vector>() {

			private static final long serialVersionUID = 1L;

			public org.apache.spark.mllib.linalg.Vector call(Row row) throws Exception {
				double doubleValues[] = new double[row.length()];
				for (int i = 0; i < row.length(); i++) {
					doubleValues[i] = Double.parseDouble(row.get(i).toString());
				}
				return org.apache.spark.mllib.linalg.Vectors.dense(doubleValues);
			}
		});
	}

	public static List<String> getUpperCaseList(List<String> list) {
		java.util.Iterator<String> iterator = list.iterator();
		List<String> upperList = new ArrayList();
		while (iterator.hasNext()) {
			upperList.add(iterator.next().toUpperCase());
		}
		return upperList;
	}

	public static Dataset<Row> removeStringColumns(Dataset<Row> dataset) {
		List<String> dataTypeList = Arrays.asList("DoubleType", "IntegerType", "LongType", "FloatType", "ShortType");
		StructType structType = dataset.schema();
		scala.collection.Iterator<StructField> iterator = structType.iterator();
		while (iterator.hasNext()) {
			StructField structField = iterator.next();
			if (!dataTypeList.contains(structField.dataType().toString())) {
				dataset = dataset.drop(structField.name());
			}
		}
		return dataset;
	}

	public static StructType vectorSchema() {
		return new StructType(
				new StructField[] { new StructField("features", new VectorUDT(), false, Metadata.empty()), });

	}

	public static Dataset<Row> selectColumns(Dataset<Row> dataset, List<String> colList) {
		String columns = "", query;
		for (int i = 0; i < colList.size(); i++) {
			if (i != colList.size() - 1) {
				columns += colList.get(i) + ",";
			} else {
				columns += colList.get(i);
			}
		}
		dataset.createOrReplaceTempView("Table");
		query = "select " + columns + " from Table";
		return session.sql(query);
	}

	public static JavaRDD<Row> cbind(JavaRDD<Row> rdd1, JavaRDD<Row> rdd2) {
		if (rdd1.count() == rdd2.count()) {
			JavaPairRDD<Long, Tuple2<Row, Row>> pairRdd = addKey(rdd1).join(addKey(rdd2));
			JavaRDD<Row> rdd = pairRdd.map(new Function<Tuple2<Long, Tuple2<Row, Row>>, Row>() {
				/**
				 * 
				 */
				private static final long serialVersionUID = 1L;

				public Row call(Tuple2<Long, Tuple2<Row, Row>> arg0) throws Exception {

					String rowStr = getStringFromRow(arg0._2._1) + "," + getStringFromRow(arg0._2._2);
					return RowFactory.create(rowStr);
				}
			});
			return rdd;

		}
		return null;

	}

	public static Dataset<Row> getVectorDataSet(Dataset<Row> dataset) {
		Dataset<Row> datasetVector = session.createDataFrame(getVectorRowRdd(dataset.toJavaRDD()), getVectorSchema());
		return datasetVector;
	}

	private static StructType getVectorSchema() {
		StructType schema = new StructType(
				new StructField[] { new StructField("features", new VectorUDT(), false, Metadata.empty()), });
		return schema;
	}

	public static JavaRDD<Row> getVectorRowRdd(JavaRDD<Row> rdd) {
		return rdd.map(new Function<Row, Row>() {
			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			public Row call(Row arg0) throws Exception {
				String[] arr = getStringFromRow(arg0).split(",");
				double[] doubleValues = (double[]) ConvertUtils.convert(arr, Double.TYPE);
				return RowFactory.create(Vectors.dense(doubleValues));
			}
		});
	}

	public static boolean validateSchemaDataType(Dataset<Row> dataset) {
		StructType structType = dataset.schema();
		Iterator<StructField> iterator = structType.iterator();
		while (iterator.hasNext()) {
			StructField structField = iterator.next();
			System.out.println(structField.dataType());
			if (!dataTypeList.contains(structField.dataType())) {
				return false;
			}
		}
		return true;
	}

	private static String getStringFromRow(Row row) {
		int len = row.length();
		String str = "";
		for (int i = 0; i < len; i++) {
			if (i != len - 1) {
				str += cleanString(row.get(i).toString()) + ",";
			} else {
				str += cleanString(row.get(i).toString());
			}
		}
		return str;

	}

	private static String cleanString(String str) {
		return str.replace("[", "").replace("]", "");
	}

	private static JavaPairRDD<Long, Row> addKey(JavaRDD<Row> rdd) {
		final GetNewKey getNewKey = new GetNewKey();
		JavaPairRDD<Long, Row> pairRdd = rdd.mapToPair(new PairFunction<Row, Long, Row>() {

			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			public Tuple2<Long, Row> call(Row arg0) throws Exception {
				return new Tuple2<Long, Row>(getNewKey.getKey(), arg0);
			}
		});
		return pairRdd;
	}

	public static StructType getLabeledPointSchema() {
		StructType schema = new StructType(
				new StructField[] { new StructField("label", DataTypes.DoubleType, false, Metadata.empty()),
						new StructField("features", new VectorUDT(), false, Metadata.empty()) });
		return schema;
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
	
	
	public static void main(String[] args) {
		SparkSession  session = SparkSession.builder().appName("DT").master("local").getOrCreate();
		
		setSession(session);
		
		String path  = "D:\\Vishal\\DataSets\\dataset_diabetes\\diabetic_data.csv";
		Dataset<Row> dataset = session.read().option("inferschema", true).option("header", true).csv(path);
		
		HashMap<String, ArrayList<String>> hashMap = divideSchema(dataset.schema());
		
		String[] columns = dataset.columns();
		ArrayList columnsList = new ArrayList(Arrays.asList(columns));
		
		ArrayList<String> featureList = new ArrayList<String>();
		featureList.add("admission_source_id");
		featureList.add("time_in_hospital");
		featureList.add("num_lab_procedures");
		featureList.add("num_procedures");
		featureList.add("num_medications");
		String lebel = "encounter_id";
		
		Dataset<Row> dataset2 = createLabledPointDataSet(dataset, lebel, featureList);
		System.out.println(dataset.first());
		System.out.println(dataset2.first());
		dataset2.show();
		session.stop();
		
	}

	
	

}

class GetNewKey1 implements Serializable {
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	long key;

	public GetNewKey1() {
		key = 0;
	}

	public long getKey() {
		return key++;
	}
}
