package textAnalytica;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.spark.ml.feature.StopWordsRemover;
import org.apache.spark.ml.feature.Tokenizer;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.types.DataTypes;

import edu.stanford.nlp.simple.Sentence;
import ml.utility.SparkMLUtility;

public class TextPreprocessing implements Serializable {

	private static Dataset<Row> dataset;

	private static SparkSession session;
	List<String> textFeatures;

	public void exec() throws Exception {

		dataset.createOrReplaceTempView("TABLE");

		ArrayList<String> featureColumns = new ArrayList<String>(Arrays.asList(dataset.columns()));
		SparkMLUtility.setSession(session);
		SparkMLUtility.toUpper(textFeatures);
		List<String> featuresToBe = textFeatures;
		if (true) {
			translateLang(textFeatures, MLConstants.EN);
		}
		if (true) {
			textFeatures = toLower(textFeatures);
		}
		if (true) {
			textFeatures = removeSpecialChar(textFeatures);
		}

		if (true) {
			textFeatures = stopWordRemover(textFeatures);
		}

		if (true) {
			textFeatures = lemmatization(textFeatures);
		}
		if (true) {
			textFeatures = thesaurus(textFeatures);
		}
		if (true) {
			textFeatures = stemming(textFeatures);
		}
		if (true) {
			textFeatures = stripWhiteSpace(textFeatures);
		}

		for (int i = 0; i < textFeatures.size(); i++) {
			dataset = dataset.withColumnRenamed(textFeatures.get(i), featuresToBe.get(i) + MLConstants.CLEANSED);
			featureColumns.add(featuresToBe.get(i) + MLConstants.CLEANSED);
		}

		dataset = SparkMLUtility.selectColumns(dataset, featureColumns);
		dataset.show();
	}

	private void translateLang(List<String> textFeatures, final String destinationLang) {

		session.udf().register(MLConstants.LANGUAGETRANSLATOR, new UDF1<String, String>() {
			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			public String call(String str) throws Exception {
				if (null != str) {
					return str;
					// return WatsonApiTranslator.languageTranslator(str,
					// destinationLang);
				} else {
					return "";
				}
			}
		}, DataTypes.StringType);

		String fucnStr = "", sqlStr = "";
		for (String feature : textFeatures) {
			fucnStr += createSqlStr(MLConstants.LANGUAGETRANSLATOR, feature, MLConstants.LANGUAGETRANSLATOR_IND);
		}
		sqlStr = "select * " + fucnStr + " from " + MLConstants.TABLE;
		dataset = session.sql(sqlStr);
	}

	private List<String> lemmatization(List<String> list) {
		String sql = "";
		List<String> newlist = new ArrayList<String>();
		for (String str : list) {
			sql += createSqlStr(MLConstants.LEMMATIZATION, str, MLConstants.LEMMATIZATION_IND);
			newlist.add(str + MLConstants.LEMMATIZATION_IND);
		}
		sql = "select * " + sql + " from table";

		session.udf().register(MLConstants.LEMMATIZATION, new UDF1<String, String>() {
			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			public String call(String str) throws Exception {
				if (null != str) {
					str = str.trim();
					if (0 != str.length()) {
						Sentence sentence = new Sentence(str);
						System.out.println(String.join(" ", sentence.lemmas()));
						return String.join(" ", sentence.lemmas());
					}
					return str;
				} else {
					return "";
				}
			}
		}, DataTypes.StringType);

		dataset.createOrReplaceTempView(MLConstants.TABLE);
		dataset = session.sql(sql);
		return newlist;
	}

	private List<String> stopWordRemover(List<String> list) {
		StopWordsRemover remover = null;
		Tokenizer tokenizer = null;
		List<String> newList = new ArrayList<String>();
		for (String str : list) {
			tokenizer = new Tokenizer().setInputCol(str).setOutputCol(str + MLConstants.TOKEN);
			dataset = tokenizer.transform(dataset);
			remover = new StopWordsRemover().setInputCol(str + MLConstants.TOKEN)
					.setOutputCol(str + MLConstants.STOP_WORD);
			dataset = remover.transform(dataset);
			newList.add(str + MLConstants.STOP_WORD);
		}
		dataset = SparkMLUtility.getStringFromArray(dataset);
		return newList;
	}

	private List<String> toLower(List<String> list) {
		String sql = "";
		List<String> newlist = new ArrayList<String>();
		for (String str : list) {
			sql += createSqlStr(MLConstants.TOLOWER, str, MLConstants.TOLOWER_IND);
			newlist.add(str + MLConstants.TOLOWER_IND);
		}
		sql = "select * " + sql + " from table";

		session.udf().register(MLConstants.TOLOWER, new UDF1<String, String>() {
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

		dataset.createOrReplaceTempView(MLConstants.TABLE);
		dataset = session.sql(sql);
		return newlist;
	}

	private List<String> removeSpecialChar(List<String> list) {
		session.udf().register(MLConstants.REMOVESPECIALCHAR, new UDF1<String, String>() {
			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			public String call(String str) throws Exception {
				if (null != str) {
					return str.replaceAll(MLConstants.SPECIALCHAR, "");
				} else {
					return "";
				}
			}
		}, DataTypes.StringType);

		dataset.createOrReplaceTempView(MLConstants.TABLE);

		String sql = "";
		List<String> newlist = new ArrayList<String>();
		for (String str : list) {
			sql += createSqlStr(MLConstants.REMOVESPECIALCHAR, str, MLConstants.REMOVESPECIALCHAR_IND);
			newlist.add(str + MLConstants.REMOVESPECIALCHAR_IND);
		}
		sql = "select * " + sql + " from table";
		dataset = session.sql(sql);
		return newlist;

	}

	private String createSqlStr(String func, String feature, String indicator) {
		return ", " + func + "(" + feature + ") as " + feature + "" + indicator;
	}

	private List<String> thesaurus(List<String> list) {
		return list;

	}

	private List<String> stemming(List<String> list) {
		return list;

	}

	private List<String> stripWhiteSpace(List<String> list) {
		session.udf().register(MLConstants.STRIPWHITESPACE, new UDF1<String, String>() {
			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			public String call(String str) throws Exception {
				if (null != str) {
					return str.replaceAll(MLConstants.WHITESPACE, " ");
				} else {
					return "";
				}
			}
		}, DataTypes.StringType);

		dataset.createOrReplaceTempView(MLConstants.TABLE);

		String sql = "";
		List<String> newlist = new ArrayList<String>();
		for (String str : list) {
			sql += createSqlStr(MLConstants.STRIPWHITESPACE, str, MLConstants.STRIPWHITESPACE_IND);
			newlist.add(str + MLConstants.STRIPWHITESPACE_IND);
		}
		sql = "select * " + sql + " from table";
		dataset = session.sql(sql);
		return newlist;
	}

	public static void main(String[] args) throws Exception {
		String path = "D:\\Vishal\\DataSets\\incident_Test.csv";
		session = SparkSession.builder().master("local").appName("Prepossing").getOrCreate();
		dataset = session.read().option("header", true).option("infraschema", true).csv(path);
		dataset.show();
		TextPreprocessing preprocessing = new TextPreprocessing();
		preprocessing.textFeatures = new ArrayList<String>();
		preprocessing.textFeatures.add("description");
		preprocessing.textFeatures.add("requested_for");
		preprocessing.exec();
	}

}