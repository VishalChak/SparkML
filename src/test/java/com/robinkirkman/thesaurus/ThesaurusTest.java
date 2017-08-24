package com.robinkirkman.thesaurus;

import org.junit.Test;

public class ThesaurusTest {
	@Test
	public void testHi() {
		String word = "Temple".toLowerCase();
		if (null != Thesaurus.getInstance().get(word)){
			System.out.println(Thesaurus.getInstance().get(word).size());
		}
		System.out.println(Thesaurus.getInstance().get(word));
	}
}
