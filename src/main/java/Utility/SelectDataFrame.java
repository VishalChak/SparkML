package Utility;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.api.java.function.Function; 
import javax.xml.bind.DatatypeConverter;
import java.sql.Timestamp;


public class SelectDataFrame {
	 private Dataset<Row> artistsAsDataFrame(JavaSparkContext sc,String input) { 
//		  String input = TestUtils.sampleArtistsDat(); 
		 
		 SQLContext sqContext = null; 
		  JavaRDD<String> data = sc.textFile(input); 
		 
		  StructType schema = DataTypes 
		    .createStructType(new StructField[] { 
		      DataTypes.createStructField("id", DataTypes.IntegerType, false), 
		      DataTypes.createStructField("name", DataTypes.StringType, false), 
		      DataTypes.createStructField("url", DataTypes.StringType, true), 
		      DataTypes.createStructField("pictures", DataTypes.StringType, true), 
		      DataTypes.createStructField("time", DataTypes.TimestampType, true) }); 
		 
		  JavaRDD<Row> rowData = data.map(new Function<String, String[]>() { 
		   public String[] call(String line) throws Exception { 
		    return line.split("\t"); 
		   } 
		  }).map(new Function<String[], Row>() { 
		   public Row call(String[] r) throws Exception { 
		    return RowFactory.create(Integer.parseInt(r[0]), r[1], r[2], r[3], 
		      new Timestamp(DatatypeConverter.parseDateTime(r[4]).getTimeInMillis())); 
		   } 
		  }); 
		 
		  return sqContext.createDataFrame(rowData, schema); 
		 } 
}
