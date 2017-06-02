package sparkML;

import java.io.Serializable;
import java.util.ArrayList;

import org.apache.commons.beanutils.ConvertUtils;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.ml.linalg.Vectors;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;

import scala.Tuple2;

public class SparkMLLib {

	public static void main(String[] args) {
		SparkSession session = SparkSession.builder().master("local").getOrCreate();
		System.setProperty("hadoop.home.dir", "D:/Vishal/hadoopWinUtill/");
		String path = "D:/Vishal/Kaggle/Titanic/test.csv";
		Dataset<Row> dataset = session.read().csv(path);
		JavaRDD<Row> javaRDD = dataset.toJavaRDD();
		JavaRDD<Row> rdd = cbind(javaRDD, javaRDD);
		rdd.foreach(new VoidFunction<Row>() {

			@Override
			public void call(Row arg0) throws Exception {
				System.out.println(arg0);
			}
		});
		session.stop();
	}

	public static JavaRDD<Row> cbind(JavaRDD<Row> rdd1, JavaRDD<Row> rdd2) {
		if (rdd1.count() == rdd2.count()) {
			JavaPairRDD<Long, Tuple2<Row, Row>> pairRdd = addKey(rdd1).join(addKey(rdd2));
			JavaRDD<Row> rdd = pairRdd.map(new Function<Tuple2<Long, Tuple2<Row, Row>>, Row>() {
				@Override
				public Row call(Tuple2<Long, Tuple2<Row, Row>> arg0) throws Exception {

					String rowStr = getStringFromRow(arg0._2._1) + "," + getStringFromRow(arg0._2._2);
					return RowFactory.create(rowStr);
				}
			});
			return rdd;

		}
		return null;

	}
	
	public static JavaRDD<Row> strRddToVectorRdd(JavaRDD<Row> rdd){
		return rdd.map(new Function<Row, Row>() {

			@Override
			public Row call(Row arg0) throws Exception {
				String [] arr = getStringFromRow(arg0).split(",");
				double[] doubleValues = (double[]) ConvertUtils.convert(arr, Double.TYPE);
				return RowFactory.create(Vectors.dense(doubleValues));
			}
		});
	}
	
	public static Dataset<Row> selectColumns(Dataset<Row> dataset, ArrayList<String> colList) {
		String colStr = "";
		for (int i = 0; i < colList.size(); i++) {
			if (i != colList.size() - 1)
				colStr = colStr + colList.get(i) + ",";
			else
				colStr = colStr + colList.get(i);
		}
		return dataset.select(colStr);
	}
	
	public static String getStringFromRow(Row row) {
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

	private static String cleanString(String str){
		return str.replace("[", "").replace("]", "");
	}
	private static JavaPairRDD<Long, Row> addKey(JavaRDD<Row> rdd) {
		final GetNewKey getNewKey = new GetNewKey();
		JavaPairRDD<Long, Row> pairRdd = rdd.mapToPair(new PairFunction<Row, Long, Row>() {

			@Override
			public Tuple2<Long, Row> call(Row arg0) throws Exception {
				return new Tuple2<Long, Row>(getNewKey.getKey(), arg0);
			}
		});
		return pairRdd;
	}

}

	class GetNewKey implements Serializable {
		long key;
	
		public GetNewKey() {
			key = 0;
		}
	
		public long getKey() {
			return key++;
		}
}
