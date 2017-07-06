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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import scala.Tuple2;
import scala.collection.Iterator;

/**
 * Added by Vishal Babu
 */

public class SparkMLUtility {
	private static final Logger logger_ = LoggerFactory.getLogger(SparkMLUtility.class);
	
	private static SparkSession session;
	
	/**
	 * @return the session
	 */
	public static SparkSession getSession() {
		return session;
	}

	/**
	 * @param session the session to set
	 */
	public static void setSession(SparkSession session) {
		SparkMLUtility.session = session;
	}


	private static List<String> dataTypeList = Arrays.asList("DoubleType", "IntegerType", "LongType", "FloatType", "ShortType");
	
	
	public static int getColumnIndex(StructType schema, String columnName) {
		String[] fieldNames = schema.fieldNames();
		for(int i = 0 ;i <fieldNames.length;i++){
			if (fieldNames[i].equalsIgnoreCase(columnName)){
				return i;
			}
		}
		return fieldNames.length-1;
		
	}
	
	public static ArrayList<String> getFeatureList(StructType schema) {
		String[] fieldNames = schema.fieldNames();
		
		ArrayList<String> featureList = new ArrayList<String>();
		for(int i = 0 ;i <fieldNames.length;i++){
			featureList.add(fieldNames[i]);
		}
		return featureList;
		
	}
	
	public static JavaRDD<Vector> getVectorMlRdd(JavaRDD<Row> rdd){
		return rdd.map(new Function<Row, Vector>() {

			private static final long serialVersionUID = 1L;

			public Vector call(Row row) throws Exception {
				String arr[] = new String [row.length()];
				for (int i = 0; i < row.length(); i++) {
					arr [i] =  row.get(i)+"";
				}
				double[] doubleValues = (double[]) ConvertUtils.convert(arr, Double.TYPE);
				return Vectors.dense(doubleValues);
			}
		});
	}
	
	
	public static JavaRDD<org.apache.spark.mllib.linalg.Vector> getVectorMlLibRdd(JavaRDD<Row> rdd){
		return rdd.map(new Function<Row, org.apache.spark.mllib.linalg.Vector>() {

			private static final long serialVersionUID = 1L;

			public org.apache.spark.mllib.linalg.Vector call(Row row) throws Exception {
				String arr[] = new String [row.length()];
				for (int i = 0; i < row.length(); i++) {
					arr [i] =  row.get(i)+"";
				}
				double[] doubleValues = (double[]) ConvertUtils.convert(arr, Double.TYPE);
				return org.apache.spark.mllib.linalg.Vectors.dense(doubleValues);
			}
		});
	}
	
	public static List<String> getUpperCaseList(List<String> list) {
		java.util.Iterator<String> iterator = list.iterator();
		List< String> upperList = new ArrayList<String>();
		while(iterator.hasNext()){
			upperList.add(iterator.next().toUpperCase());
		}
		return upperList;
	}
	
	public static Dataset<Row> removeStringColumns(Dataset<Row> dataset) {
		StructType structType = dataset.schema();
		Iterator<StructField> iterator = structType.iterator();
		while (iterator.hasNext()) {
			StructField structField = iterator.next();
			if (!dataTypeList.contains(structField.dataType()+"")) {
				dataset = dataset.drop(structField.name());
			}
		}
		return dataset;
	}
	
	
	public static StructType vectorSchema(){
		return new StructType(
				new StructField[] { new StructField("features", new VectorUDT(), false, Metadata.empty()), });
		
	}
	
	public static Dataset<Row> selectColumns(Dataset<Row> dataset, List<String> colList) {
		String columns = "", query;
		for(int i = 0;i<colList.size();i++){
			if (i!= colList.size()-1){
				columns+=colList.get(i)+",";
			} else {
				columns+=colList.get(i);
			}
		}
		dataset.createOrReplaceTempView("Table");
		query = "select "+columns+" from Table";
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
		JavaRDD<Row> rdd = dataset.javaRDD().map(new Function<Row, Row>() {

			public Row call(Row arg0) throws Exception {
				String[] arr = new String[arg0.size()];
				for(int i=0;i<arg0.size();i++){
					arr[i] = arg0.get(i)+"";
				}
				double[] doubleValues = (double[]) ConvertUtils.convert(arr, Double.TYPE);
				return RowFactory.create(Vectors.dense(doubleValues));
			}
		});
		return session.createDataFrame(rdd, getVectorSchema());
		
	}

	private static StructType getVectorSchema() {
		StructType schema = new StructType(
				new StructField[] { new StructField("features", new VectorUDT(), false, Metadata.empty()), });
		return schema;
	}

	public static JavaRDD<Row> getVectorRowRdd(JavaRDD<Row> rdd) {
		return rdd.map(new Function<Row, Row>() {
			
			private static final long serialVersionUID = 1L;

			public Row call(Row arg0) throws Exception {
				String[] arr = getStringFromRow(arg0).split(",");
				double[] doubleValues = (double[]) ConvertUtils.convert(arr, Double.TYPE);
				return RowFactory.create(Vectors.dense(doubleValues));
			}
		});
	}

	public static boolean validateSchemaDataType(Dataset<Row> dataset){
		StructType structType = dataset.schema();
		Iterator<StructField> iterator = structType.iterator();
		while (iterator.hasNext()){
			StructField structField = iterator.next();
			
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
				str += cleanString(row.get(i)+"") + ",";
			} else {
				str += cleanString(row.get(i)+"");
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
	
	private static StructType getLabeledPointSchema() {
		StructType schema = new StructType(new StructField[]{
			    new StructField("label", DataTypes.DoubleType, false, Metadata.empty()),
			    new StructField("features", new VectorUDT(), false, Metadata.empty())
			});
		return schema;
	}
	
	
	public static Dataset<Row> createLabledPointDataSet(Dataset<Row> dataset, String targetCol,
			 List<String> featureColumns) {
		
		ArrayList<String> columnsList = new ArrayList<String>(Arrays.asList(dataset.columns()));
		final ArrayList<Integer> featureIntList = new ArrayList<Integer>(); 
		for(int i=0;i<columnsList.size();i++){
			columnsList.set(i, columnsList.get(i).trim().toLowerCase());
		}
		for(int i = 0 ; i < featureColumns.size(); i++){
			featureIntList.add(columnsList.indexOf(featureColumns.get(i).trim().toLowerCase()));
		}
		final int labeledIndex = columnsList.indexOf(targetCol.toLowerCase());
		
		JavaRDD<Row> rdd = dataset.toJavaRDD().map(new Function<Row, Row>() {
			private static final long serialVersionUID = 1L;

			public Row call(Row arg0) throws Exception {
				String[] arr = new String[featureIntList.size()];
				for(int i = 0 ; i < featureIntList.size();i++){
					String atr = arg0.get(featureIntList.get(i))+"";
					arr[i] = atr;
				}
				double lable = (Double) ConvertUtils.convert(arg0.get(labeledIndex)+"", Double.TYPE);
				double[] doubleValues = (double[]) ConvertUtils.convert(arr, Double.TYPE);
				return RowFactory.create(lable, Vectors.dense(doubleValues));
				
			}
		});
		return session.createDataFrame(rdd, getLabeledPointSchema());
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

}

class GetNewKey implements Serializable {
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	long key;

	public GetNewKey() {
		key = 0;
	}

	public long getKey() {
		return key++;
	}
}