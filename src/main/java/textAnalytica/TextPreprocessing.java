package textAnalytica;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

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
	private static List<String> textFeatures;

	public void exec() throws Exception {

		dataset.createOrReplaceTempView("TABLE");

		ArrayList<String> featureColumns = new ArrayList<String>(Arrays.asList(dataset.columns()));
		SparkMLUtility.setSession(session);
		SparkMLUtility.toUpper(textFeatures);
		List<String> featuresToBe = textFeatures;
		textFeatures = textCleansing(textFeatures);
		
		
		
		for (int i = 0; i < textFeatures.size(); i++) {
			dataset = dataset.withColumnRenamed(textFeatures.get(i), featuresToBe.get(i) + MLConstants.CLEANSED);
			featureColumns.add(featuresToBe.get(i) + MLConstants.CLEANSED);
		}

		dataset = SparkMLUtility.selectColumns(dataset, featureColumns);
		dataset.show();
		dataset.write().json("D:\\Data\\res");
	}



	private List<String> textCleansing(List<String> list) {

		final String stopWordRegex = stopWordRegex();

		session.udf().register(MLConstants.TEXTCLEANSEING, new UDF1<String, String>() {
			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			public String call(String str) throws Exception {
				if (null != str) {
					if (true) {
						// str = WatsonApiTranslator.languageTranslator(str,
						// MLConstants.EN);
					}
					if (true) {
						str = str.toLowerCase();
					}
					if (true) {
						str = str.replaceAll(MLConstants.SPECIALCHAR, "");
					}
					if (true) {
						str = str.replaceAll(stopWordRegex, "");
					}

					if (true) {
						str = str.trim();
						if (str.length()>0){
							System.out.println(str);
							Sentence sentence = new Sentence(str);
							str = String.join(" ",sentence.lemmas());
						}
					}
					if (true) {
						
						// textFeatures = thesaurus(textFeatures);
					}
					if (true) {
						// textFeatures = stemming(textFeatures);
					}
					if (true) {
						str = str.replaceAll(MLConstants.WHITESPACE, "");
					}
					return str.trim();
				} else {
					return "";
				}
			}
		}, DataTypes.StringType);

		dataset.createOrReplaceTempView(MLConstants.TABLE);

		String sql = "";
		List<String> newlist = new ArrayList<String>();
		SparkMLUtility.toUpper(list);
		for (String str : list) {
			sql += createSqlStr(MLConstants.TEXTCLEANSEING, str, MLConstants.CLEANSED);
			newlist.add(str + MLConstants.CLEANSED);
		}
		sql = "select * " + sql + " from table";
		dataset = session.sql(sql);
		return newlist;
	}
	
	private String stopWordRegex() {
		String stopWords = "a,able,about,across,after,all,almost,also,am,among,an,and,any,are,as,at,be,because,been,but,by,can,cannot,could,dear,did,do,does,either,else,ever,every,for,from,get,got,had,has,have,he,her,hers,him,his,how,however,i,if,in,into,is,it,its,just,least,let,like,likely,may,me,might,most,must,my,neither,no,nor,not,of,off,often,on,only,or,other,our,own,rather,said,say,says,she,should,since,so,some,than,that,the,their,them,then,there,these,they,this,tis,to,too,twas,us,wants,was,we,were,what,when,where,which,while,who,whom,why,will,with,would,yet,you,your,'s,'ve";
		return "\\b( ?" + stopWords.replace(",", "|") + ")\\b";
	}
	
	private String createSqlStr(String func, String feature, String indicator) {
		return ", " + func + "(" + feature + ") as " + feature + "" + indicator;
	}

	public static void main(String[] args) throws Exception {
		System.out.println(System.getProperty("java.runtime.version"));
		session = SparkSession.builder().appName("Tere").master("local").getOrCreate();
		String path = "D:\\Vishal\\DataScience\\DataSets\\incident_new.csv";
		dataset = session.read().option("header", true).csv(path);
		textFeatures = new ArrayList<String>();
		textFeatures.add("description");
		textFeatures.add("requested_for");
		TextPreprocessing  preprocessing = new TextPreprocessing();
		preprocessing.exec();
		
	}

}