package com.robinkirkman.thesaurus;

import java.util.Arrays;

import org.junit.Test;

public class ThesaurusTest {
	@Test
	public void testHi() {
		String word = "temple".toLowerCase();
		if (null != Thesaurus.getInstance().get(word)){
			System.out.println(Thesaurus.getInstance().get(word).size());
//			Arrays.sort();
		}
		System.out.println(Thesaurus.getInstance().get(word));
	}
}
