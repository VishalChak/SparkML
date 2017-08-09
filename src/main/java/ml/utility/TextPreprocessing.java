package ml.utility;

import java.util.ArrayList;
import java.util.List;

import org.apache.spark.ml.feature.StopWordsRemover;
import org.apache.spark.ml.feature.Tokenizer;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.types.DataTypes;

import edu.stanford.nlp.simple.Sentence;

public class TextPreprocessing {

	private static SparkSession session = null;
	private static Dataset<Row> dataset = null;

	public static void main(String[] args) {
		session = new SparkSession.Builder().appName("preprocessing").master("local").getOrCreate();

		String path = "D:\\Vishal\\DataSets\\incident_Test.csv";
		dataset = session.read().option("header", "true").option("inferSchema", "true").csv(path);

		List<String> list = new ArrayList<String>();
		list.add("description");
		list.add("requested_for");
		
		if (true) {
			list = toLower(list);
		}
		if (true) {
			list = removeSpecialChar(list);
		}
		if (true) {
			list = stopWordRemover(list);
		}
		if (true){
			list = lemmatization(list);
		}
		dataset.show();
	}

	private static List<String> thesaurus(List<String> list) {
		return list;

	}

	private static List<String> lemmatization(List<String> list) {
		String sql = "";
		List<String> newlist = new ArrayList<String>();
		for (String str : list) {
			sql += createSqlStr("lemmatization", str, "_lem");
			newlist.add(str + "_lem");
		}
		sql = "select * " + sql + " from table";

		session.udf().register("lemmatization", new UDF1<String, String>() {
			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			public String call(String str) throws Exception {
				if (null != str && str.trim() != "") {
					System.out.println(str);
					Sentence sentence = new Sentence(str);
					System.out.println(sentence.lemmas());
					return str.toLowerCase();
				} else {
					return "";
				}
			}
		}, DataTypes.StringType);

		dataset.createOrReplaceTempView("table");
		dataset = session.sql(sql);
		return newlist;
	}

	private static List<String> stemming(List<String> list) {
		return list;

	}

	private static List<String> stripWhiteSpace(List<String> list) {
		return list;
	}

	private static List<String> stopWordRemover(List<String> list) {
		StopWordsRemover remover = null;
		Tokenizer tokenizer = null;
		List<String> newList = new ArrayList<String>();
		for (String str : list) {
			tokenizer = new Tokenizer().setInputCol(str).setOutputCol(str + "_tk");
			dataset = tokenizer.transform(dataset);
			remover = new StopWordsRemover().setInputCol(str + "_tk").setOutputCol(str + "_swr");
			dataset = remover.transform(dataset);
			newList.add(str + "_swr");
		}
		SparkMLUtility.setSession(session);
		dataset = SparkMLUtility.getStringFromArray(dataset);
		return newList;
	}

	private static List<String> toLower(List<String> list) {
		String sql = "";
		List<String> newlist = new ArrayList<String>();
		for (String str : list) {
			sql += createSqlStr("toLower", str, "_tl");
			newlist.add(str + "_tl");
		}
		sql = "select * " + sql + " from table";

		session.udf().register("toLower", new UDF1<String, String>() {
			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			public String call(String str) throws Exception {
				if (null != str) {
					return str.toLowerCase();
				} else {
					return "";
				}
			}
		}, DataTypes.StringType);

		dataset.createOrReplaceTempView("table");
		dataset = session.sql(sql);

		return newlist;
	}

	private static List<String> removeSpecialChar(List<String> list) {
		session.udf().register("removeSpecialChar", new UDF1<String, String>() {
			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			public String call(String str) throws Exception {
				if (null != str) {
					return str.replaceAll("[^a-zA-Z0-9 .]", "");
				} else {
					return "";
				}
			}
		}, DataTypes.StringType);

		dataset.createOrReplaceTempView("table");

		String sql = "";
		List<String> newlist = new ArrayList<String>();
		for (String str : list) {
			sql += createSqlStr("removeSpecialChar", str, "_rsc");
			newlist.add(str + "_rsc");
		}
		sql = "select * " + sql + " from table";
		dataset = session.sql(sql);
		return newlist;

	}

	private static String createSqlStr(String func, String feature, String indicator) {
		return ", " + func + "(" + feature + ") as " + feature + "" + indicator;
	}
}