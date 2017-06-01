package sparkML;

import java.util.Arrays;
import java.util.List;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import scala.collection.Iterator;

public class DataTypeValidator {
	
	static List<String> dataTypeList = Arrays.asList("DoubleType", "IntegerType", "LongType", "FloatType", "ShortType");
	public static boolean validate(Dataset<Row> dataset){
		StructType structType = dataset.schema();
		Iterator<StructField> iterator = structType.iterator();
		while (iterator.hasNext()){
			StructField structField = iterator.next();
			System.out.println(structField.dataType());
			if (!dataTypeList.contains(structField.dataType())) {
				return false;
			}
		}
		return true;
	}
}
