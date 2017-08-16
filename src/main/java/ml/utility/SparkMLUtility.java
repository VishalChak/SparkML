package ml.utility;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;

import org.apache.commons.beanutils.ConvertUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.ml.feature.VectorAssembler;
import org.apache.spark.ml.linalg.DenseVector;
import org.apache.spark.ml.linalg.SparseVector;
import org.apache.spark.ml.linalg.Vector;
import org.apache.spark.ml.linalg.VectorUDT;
import org.apache.spark.ml.linalg.Vectors;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.types.ArrayType;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import scala.Tuple2;
import scala.collection.Iterator;
import scala.collection.mutable.WrappedArray;
import textAnalytica.EndToEnd;

/**
 * Added by Vishal Babu
 */

public class SparkMLUtility {

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
		SparkMLUtility.session = session;
	}

	private static final Logger logger_ = LoggerFactory.getLogger(SparkMLUtility.class);

	private static SparkSession session;

	private static List<String> dataTypeList = Arrays.asList("DoubleType", "IntegerType", "LongType", "FloatType",
			"ShortType");

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
				String arr[] = new String[row.length()];
				for (int i = 0; i < row.length(); i++) {
					arr[i] = row.get(i) + "";
				}
				double[] doubleValues = (double[]) ConvertUtils.convert(arr, Double.TYPE);
				return Vectors.dense(doubleValues);
			}
		});
	}

	public static JavaRDD<org.apache.spark.mllib.linalg.Vector> getVectorMlLibRdd(JavaRDD<Row> rdd) {
		return rdd.map(new Function<Row, org.apache.spark.mllib.linalg.Vector>() {

			private static final long serialVersionUID = 1L;

			public org.apache.spark.mllib.linalg.Vector call(Row row) throws Exception {
				String arr[] = new String[row.length()];
				for (int i = 0; i < row.length(); i++) {
					arr[i] = row.get(i) + "";
				}
				double[] doubleValues = (double[]) ConvertUtils.convert(arr, Double.TYPE);
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
		StructType structType = dataset.schema();
		Iterator<StructField> iterator = structType.iterator();
		while (iterator.hasNext()) {
			StructField structField = iterator.next();
			if (!dataTypeList.contains(structField.dataType() + "")) {
				dataset = dataset.drop(structField.name());
			}
		}
		return dataset;
	}

	public static Dataset<Row> cBindDataset(Dataset<Row> dataset1, Dataset<Row> dataset2) {
		String[] columns1 = dataset1.columns();
		String[] columns2 = dataset2.columns();
		String str = "";

		for (int i = 0; i < columns1.length; i++) {
			if (i != columns1.length - 1) {
				str += "a1." + columns1[i] + ",";
			} else {
				str += "a1." + columns1[i];
			}
		}

		for (int i = 0; i < columns2.length; i++) {
			str += ",a2." + columns2[i];
		}
		System.out.println(str);
		dataset1.createOrReplaceTempView("table1");
		dataset2.createOrReplaceTempView("table2");
		Dataset<Row> sql = session.sqlContext().sql("select " + str
				+ " from (SELECT a.*,row_number() over (partition by 1 order by 1) as key1 FROM table1 a) a1 left outer join (SELECT b.*,row_number() over (partition by 1 order by 1) as key2 FROM table2 b) a2 on a1.key1=a2.key2");
		return sql;
	}

	public static Dataset<Row> selectColumns(Dataset<Row> dataset, List<String> colList) {
		String columns = "", query;
		columns = String.join(", ", colList);
		dataset.createOrReplaceTempView("table");
		query = "select " + columns + " from table";
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
		@SuppressWarnings("serial")
		JavaRDD<Row> rdd = dataset.javaRDD().map(new Function<Row, Row>() {

			public Row call(Row arg0) throws Exception {
				String[] arr = new String[arg0.size()];
				for (int i = 0; i < arg0.size(); i++) {
					arr[i] = arg0.get(i) + "";
				}
				double[] doubleValues = (double[]) ConvertUtils.convert(arr, Double.TYPE);
				return RowFactory.create(Vectors.dense(doubleValues));
			}
		});
		return session.createDataFrame(rdd, schemaVector());

	}

	public static StructType schemaVector() {
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

	public static boolean validateSchemaDataType(Dataset<Row> dataset) {
		StructType structType = dataset.schema();
		Iterator<StructField> iterator = structType.iterator();
		while (iterator.hasNext()) {
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
				str += cleanString(row.get(i) + "") + ",";
			} else {
				str += cleanString(row.get(i) + "");
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

	private static StructType schemaLabeledPoint() {
		StructType schema = new StructType(
				new StructField[] { new StructField("label", DataTypes.DoubleType, false, Metadata.empty()),
						new StructField("features", new VectorUDT(), false, Metadata.empty()) });
		return schema;
	}

	public static Dataset<Row> createLabledPointDataSet(Dataset<Row> dataset, String targetCol,
			List<String> featureColumns) {

		ArrayList<String> columnsList = new ArrayList<String>(Arrays.asList(dataset.columns()));
		final ArrayList<Integer> featureIntList = new ArrayList<Integer>();
		for (int i = 0; i < columnsList.size(); i++) {
			columnsList.set(i, columnsList.get(i).trim().toLowerCase());
		}
		for (int i = 0; i < featureColumns.size(); i++) {
			featureIntList.add(columnsList.indexOf(featureColumns.get(i).trim().toLowerCase()));
		}
		final int labeledIndex = columnsList.indexOf(targetCol.toLowerCase());

		JavaRDD<Row> rdd = dataset.toJavaRDD().map(new Function<Row, Row>() {
			private static final long serialVersionUID = 1L;

			public Row call(Row arg0) throws Exception {
				String[] arr = new String[featureIntList.size()];
				for (int i = 0; i < featureIntList.size(); i++) {
					String atr = arg0.get(featureIntList.get(i)) + "";
					arr[i] = atr;
				}
				double lable = (Double) ConvertUtils.convert(arg0.get(labeledIndex) + "", Double.TYPE);
				double[] doubleValues = (double[]) ConvertUtils.convert(arr, Double.TYPE);
				return RowFactory.create(lable, Vectors.dense(doubleValues));

			}
		});
		return session.createDataFrame(rdd, schemaLabeledPoint());
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

		while (iterator.hasNext()) {
			StructField field = iterator.next();
			if (numericDatatypes.contains(field.dataType())) {
				numColumns.add(field.name());
			} else {
				otherColumns.add(field.name());
			}
		}
		HashMap<String, ArrayList<String>> map = new HashMap<String, ArrayList<String>>();
		map.put("numColumns", numColumns);
		map.put("otherColumns", otherColumns);
		return map;
	}

	public static HashMap<String, String> getColumnDetails(Dataset<Row> dataset, ArrayList<String> listOfColumns) {
		dataset.show();
		HashMap<String, String> columnDetails = new HashMap<String, String>();
		StructType structType = dataset.schema();
		StructField[] structField = structType.fields();
		if (null != listOfColumns) {
			for (String columnName : listOfColumns) {
				for (StructField field : structField) {
					if ((field.name()).equalsIgnoreCase(columnName)) {
						columnDetails.put(columnName,
								StringUtils.remove(field.dataType().toString(), "Type").toUpperCase());
					}
				}
			}
		}
		return columnDetails;
	}

	private static StructType schemaDouble(int len, Object featureCols, String tagetCol) {
		StructField fields[] = new StructField[len];
		boolean notNull = false, isMap = false;
		Map<String, DataType> featureDataTypeMap = null;
		Object[] keyArr = null;
		ArrayList<String> featureList = null;

		if (null != featureCols && featureCols instanceof Map<?, ?>) {
			HashMap<String, String> featureMap = (HashMap<String, String>) featureCols;
			featureDataTypeMap = new HashMap();
			keyArr = featureMap.keySet().toArray();
			for (String key : featureMap.keySet()) {
				featureDataTypeMap.put(key, getDataTypeFromSStr(featureMap.get(key)));
			}
			isMap = true;
			notNull = true;
		} else if (null != featureCols) {
			featureList = (ArrayList<String>) featureCols;
			notNull = true;
		}

		for (int i = 0; i < len; i++) {
			if (i == 0) {
				if (tagetCol != "") {
					fields[i] = new StructField(tagetCol, DataTypes.DoubleType, false, Metadata.empty());
				} else {
					fields[i] = new StructField("Feature" + i, DataTypes.DoubleType, false, Metadata.empty());
				}
			} else {
				if (notNull && isMap) {
					fields[i] = new StructField(keyArr[i - 1] + "", featureDataTypeMap.get(keyArr[i - 1]), false,
							Metadata.empty());
				} else if (notNull && !isMap) {
					fields[i] = new StructField(featureList.get(i - 1), DataTypes.DoubleType, false, Metadata.empty());
				} else {
					fields[i] = new StructField("Feature" + i, DataTypes.DoubleType, false, Metadata.empty());
				}
			}

		}
		StructType schema = new StructType(fields);

		System.out.println(schema.size());
		return schema;
	}

	private static DataType getDataTypeFromSStr(String dataType) {

		if (null != dataType) {
			dataType = dataType.trim();
		}

		if (dataType.equalsIgnoreCase("integer")) {
			return DataTypes.IntegerType;
		} else if (dataType.equalsIgnoreCase("long")) {
			return DataTypes.LongType;
		} else if (dataType.equalsIgnoreCase("float")) {
			return DataTypes.FloatType;
		} else if (dataType.equalsIgnoreCase("short")) {
			return DataTypes.ShortType;
		} else if (dataType.equalsIgnoreCase("string")) {
			return DataTypes.StringType;
		} else {
			return DataTypes.DoubleType;
		}
	}

	@SuppressWarnings("unused")
	public static Dataset<Row> createRowDSFromLablePointDS(Dataset<Row> dataset, Object featureCols, String targetCol) {

		StructField[] structFields = dataset.schema().fields();
		final int featureInx;
		final int lableInx;
		StructField structField = structFields[0];
		if (structField.dataType() != DataTypes.DoubleType) {
			featureInx = 0;
			lableInx = 1;
		} else {
			featureInx = 1;
			lableInx = 0;
		}

		@SuppressWarnings("serial")
		JavaRDD<Row> rdd = dataset.toJavaRDD().map(new Function<Row, Row>() {
			public Row call(Row arg0) throws Exception {
				DenseVector denseVector = (DenseVector) arg0.get(featureInx);
				double[] values = denseVector.values();
				Object[] arr = new Double[values.length + 1];
				arr[0] = (Double) ConvertUtils.convert(arg0.get(lableInx) + "", Double.TYPE);
				for (int i = 0; i < values.length; i++) {
					arr[i + 1] = values[i];
				}
				return RowFactory.create(arr);
			}
		});

		Row first = dataset.first();
		DenseVector denseVector = (DenseVector) first.get(featureInx);
		double[] values = denseVector.values();
		StructType schemaDouble = schemaDouble(values.length + 1, featureCols, targetCol);
		Dataset<Row> datasetRow = session.createDataFrame(rdd, schemaDouble);
		return datasetRow;
	}

	public Dataset<Row> getVectorFromDouble(Dataset<Row> dataset) {
		return null;
	}

	private static Dataset<Row> getDoubleFromVector(Dataset<Row> dataset, List<String> vecColList) {

		StructType schema = dataset.schema();
		StructField[] fields = dataset.schema().fields();
		StructField[] structFields = new StructField[schema.size()];
		final ArrayList<Integer> indxList = new ArrayList<Integer>();

		for (int i = 0; i < fields.length; i++) {
			if (fields[i].dataType() instanceof org.apache.spark.mllib.linalg.VectorUDT
					|| fields[i].dataType() instanceof VectorUDT) {
				structFields[i] = new StructField(fields[i].name(), new ArrayType(DataTypes.DoubleType, false), true,
						Metadata.empty());
				indxList.add(schema.indexOf(fields[i]));
			} else {
				structFields[i] = fields[i];
			}

		}

		JavaRDD<Row> rdd = dataset.toJavaRDD().map(new Function<Row, Row>() {

			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			public Row call(Row arg0) throws Exception {
				Object newRow[] = new Object[arg0.size()];
				for (int i = 0; i < arg0.size(); i++) {
					if (indxList.contains(i) && null != arg0.get(i)) {
						DenseVector denseVector = null;
						SparseVector sparseVector = null;
						double arr[] = null;
						if (arg0.getAs(i) instanceof DenseVector) {
							denseVector = arg0.getAs(i);
							arr = denseVector.toArray();
						} else {
							sparseVector = arg0.getAs(i);
							arr = sparseVector.toArray();
						}
						newRow[i] = arr;
					} else if (null != arg0.get(i)) {
						newRow[i] = arg0.getAs(i);
					}

				}
				return RowFactory.create(newRow);
			}
		});

		schema = new StructType(structFields);
		Dataset<Row> dataFrame = session.createDataFrame(rdd, schema);

		return dataFrame;
	}

	private static Dataset<Row> getVectorFromDouble(Dataset<Row> dataset, List<String> vecColList) {

		StructType schema = dataset.schema();
		StructField[] fields = dataset.schema().fields();
		StructField[] structFields = new StructField[schema.size()];
		final ArrayList<Integer> indxList = new ArrayList<Integer>();

		for (int i = 0; i < fields.length; i++) {
			if (fields[i].dataType() instanceof ArrayType) {
				ArrayType arrayType = (ArrayType) fields[i].dataType();
				if (DataTypes.DoubleType.equals(arrayType.elementType())) {
					structFields[i] = new StructField(fields[i].name(), new VectorUDT(), true, Metadata.empty());
					indxList.add(schema.indexOf(fields[i]));
				} else {
					structFields[i] = fields[i];
				}

			} else {
				structFields[i] = fields[i];
			}
		}
		schema = new StructType(structFields);
		JavaRDD<Row> rdd = dataset.toJavaRDD().map(new Function<Row, Row>() {

			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			public Row call(Row arg0) throws Exception {
				Object newRow[] = new Object[arg0.size()];
				for (int i = 0; i < arg0.size(); i++) {

					if (indxList.contains(i) && null != arg0.get(i)) {
						WrappedArray<Double> array = arg0.getAs(i);
						Iterator<Double> iterator = array.iterator();
						double[] arr = new double[array.size()];
						int count = 0;
						while (iterator.hasNext()) {
							arr[count++] = iterator.next();
						}
						newRow[i] = Vectors.dense(arr);
					} else {
						newRow[i] = arg0.getAs(i);
					}

				}
				return RowFactory.create(newRow);
			}
		});
		dataset = session.createDataFrame(rdd, schema);
		return dataset;
	}

	public static Dataset<Row> getStringFromArray(Dataset<Row> dataset) {
		StructType schema = dataset.schema();
		StructField[] fields = dataset.schema().fields();
		StructField[] structFields = new StructField[schema.size()];
		final ArrayList<Integer> indxList = new ArrayList<Integer>();

		for (int i = 0; i < fields.length; i++) {
			if (fields[i].dataType() instanceof ArrayType) {
				ArrayType arrayType = (ArrayType) fields[i].dataType();
				if (DataTypes.StringType.equals(arrayType.elementType())) {
					structFields[i] = new StructField(fields[i].name(), DataTypes.StringType, true, Metadata.empty());
					indxList.add(schema.indexOf(fields[i]));
				} else {
					structFields[i] = fields[i];
				}

			} else {
				structFields[i] = fields[i];
			}
		}
		schema = new StructType(structFields);
		JavaRDD<Row> rdd = dataset.toJavaRDD().map(new Function<Row, Row>() {

			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			public Row call(Row arg0) throws Exception {
				Object newRow[] = new Object[arg0.size()];
				for (int i = 0; i < arg0.size(); i++) {

					if (indxList.contains(i) && null != arg0.get(i)) {
						WrappedArray<String> array = arg0.getAs(i);
						Iterator<String> iterator = array.iterator();
						String str = "";
						while (iterator.hasNext()) {
							str += " " + iterator.next();
						}
						newRow[i] = str;
					} else {
						newRow[i] = arg0.getAs(i);
					}

				}
				return RowFactory.create(newRow);
			}
		});

		dataset = session.createDataFrame(rdd, schema);
		return dataset;

	}

	public static Dataset<Row> createLablePoint(Dataset<Row> dataset, String targetCol, List<String> featureColumns) {
		String[] arr = new String[featureColumns.size()];
		arr = featureColumns.toArray(arr);
		VectorAssembler assembler = new VectorAssembler().setInputCols(arr).setOutputCol("features");
		dataset = assembler.transform(dataset);
		dataset = dataset.select(targetCol, "features");
		dataset = dataset.withColumnRenamed(targetCol, "label");
		return dataset;
	}
	
	private static void toDouble(List<String> textFeatures, Dataset<Row> dataset) {
		session.udf().register("toDouble", new UDF1<Integer, Double>() {
			public Double call(Integer t1) throws Exception {
				return new Double(t1);
			}
		}, DataTypes.DoubleType);
		
		String fucnStr = "", sqlStr = "";
		for (String feature : textFeatures) {
			fucnStr += createSqlStr("toDouble", feature, "DB");
		}
		sqlStr = "select * " + fucnStr + " from " + "table";
		System.out.println(sqlStr);
		dataset.createOrReplaceTempView("table");
		dataset = session.sql(sqlStr);
		dataset.show();
	}
	
	private static String createSqlStr(String func, String feature, String indicator) {
		return ", " + func + "(" + feature + ") as " + feature + "_" + indicator;
	}

	public static void main(String[] args) {
		SparkSession session = SparkSession.builder().appName("ENN").master("local").getOrCreate();
		Dataset<Row> dataset = EndToEnd.getFinalData(session);
		setSession(session);
		String path = "D:\\Vishal\\DataSets\\Data\\train.csv";
		Dataset<Row> dataset2 = session.read().option("header", "true").option("inferSchema", "true").csv(path);
		dataset2 = removeStringColumns(dataset2);
//		dataset2.show();
		VectorAssembler assembler = new VectorAssembler()
			      .setInputCols(new String[]{"PassengerId", "Survived"})
			      .setOutputCol("features");
		Dataset<Row> transform = assembler.transform(dataset2);
		transform = transform.withColumnRenamed("Survived", "label");
		transform = transform.select("features", "label");
		List< String>list = new ArrayList<String>();
		list.add("label");
		toDouble(list, transform);
		
	}
	
	public static void toUpper(List<String> featureList) {
		ListIterator<String> iterator = featureList.listIterator();
		while (iterator.hasNext()) {
			iterator.set(iterator.next().toUpperCase());
		}
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
