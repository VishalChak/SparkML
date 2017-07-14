package textAnalytica;

import org.apache.commons.lang.StringUtils;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SparkSession;


public class test {
	public static void main(String[] args) {
		SparkSession session = SparkSession.builder().master("local").appName("K_MEANS").getOrCreate();
		String path = "D:\\Vishal\\DataSets\\Iris\\Test.csv";
		String path1 = "D:\\Vishal\\DataSets\\Iris\\test2.csv";
		Dataset<Row> dataset = session.read().option("inferschema", true).csv(path);
		Dataset<Row> dataset2 = session.read().option("inferschema", true).csv(path1);
		/*SQLContext sqlContext = session.sqlContext();
		dataset.show();
		dataset2.show();
		dataset.createOrReplaceTempView("table1");
		dataset2.createOrReplaceTempView("table2");
		String str = "";
		
		str = "a1._c0,a1._c1,a1._c2,a1._c3, a2._c0";
		Dataset<Row> sql = sqlContext.sql("select "+str+" from (SELECT a.*,row_number() over (partition by 1 order by 1) as key1 FROM table1 a) a1 left outer join (SELECT b.*,row_number() over (partition by 1 order by 1) as key2 FROM table2 b) a2 on a1.key1=a2.key2");
		*/
		Dataset<Row> cBindDataset = cBindDataset(session, dataset, dataset2);
		cBindDataset.show();
		session.stop();
		
	}
	
	
	private static Dataset<Row> cBindDataset(SparkSession session ,Dataset<Row> dataset1, Dataset<Row> dataset2) {
		String[] columns1 = dataset1.columns();
		String[] columns2 = dataset2.columns();
		String str = "";
		
		for (int i =0 ;i<columns1.length;i++){
			if (i!=columns1.length-1){
				str+="a1."+columns1[i]+",";
			} else {
				str+="a1."+columns1[i];
			}
		}
		System.out.println(str);
		for (int i =0 ;i<columns2.length;i++){
			str+=",a2."+columns1[i];
		}
		
		dataset1.createOrReplaceTempView("table1");
		dataset2.createOrReplaceTempView("table2");
		
		System.out.println(str);
		Dataset<Row> sql = session.sqlContext().sql("select "+str+" from (SELECT a.*,row_number() over (partition by 1 order by 1) as key1 FROM table1 a) a1 left outer join (SELECT b.*,row_number() over (partition by 1 order by 1) as key2 FROM table2 b) a2 on a1.key1=a2.key2");
		return sql;
	}
}
