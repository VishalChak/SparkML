package com.robinkirkman.thesaurus;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Thesaurus {

	private static Thesaurus instance;
	
	public static synchronized Thesaurus getInstance() {
		if(instance == null)
			instance = new Thesaurus();
		return instance;
	}

	private Map<String, Long> map;
	
	private Thesaurus() {
		map = new HashMap<String, Long>();
		InputStream in = Thesaurus.class.getResourceAsStream("mthesaur.txt");
		try {
			long idx = 0;
			long count = 0;
			StringBuilder sb = new StringBuilder();
			boolean nl = true;
			for(int b = in.read(); b >= 0; b = in.read()) {
				boolean dump = false;
				char c = (char) b;
				if(c == '\n') {
					idx = count+1;
					nl = true;
					sb = new StringBuilder();
				} else if(c == ',')
					dump = true;
				else if(c != '\r')
					sb.append(c);
				if(dump && nl) {
					map.put(sb.toString(), idx);
					nl = false;
				}
				count++;
			}
			in.close();
		} catch(IOException e) {
			throw new RuntimeException(e);
		}
	}
	
	public List<String> get(String word) {
		if(!map.containsKey(word))
			return null;
		InputStream in = Thesaurus.class.getResourceAsStream("mthesaur.txt");
		try {
			in.skip(map.get(word));
			StringBuilder sb = new StringBuilder();
			List<String> syn = new ArrayList<String>();
			for(int b = in.read(); b >= 0; b = in.read()) {
				char c = (char) b;
				if(c == '\n')
					break;
				if(c == ',') {
					syn.add(sb.toString());
					sb = new StringBuilder();
				} else if(c != '\r')
					sb.append(c);
			}
			if(sb.length() > 0)
				syn.add(sb.toString());
			in.close();
			return syn;
		} catch(IOException e) {
			throw new RuntimeException(e);
		}
	}
}
