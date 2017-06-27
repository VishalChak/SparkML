package Utility;

import java.util.ArrayList;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

public class SelectCol {
	public static Dataset<Row> getcol(Dataset<Row> dataset, ArrayList<String> colList){
		String colStr = "";
		for(int i=0;i<colList.size();i++){
			if(i!=colList.size()-1)
				colStr = colStr+colList.get(i)+",";
			else
				colStr = colStr+colList.get(i);
		}
		return dataset.select(colStr);
	}
}