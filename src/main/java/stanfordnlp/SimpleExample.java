package stanfordnlp;

import java.io.IOException;
import java.util.List;

import edu.stanford.nlp.simple.*;

public class SimpleExample {

	public static void main(String[] args) throws IOException {
		/*Properties props = new Properties();
		props.put("annotators", "tokenize, ssplit, pos, lemma, ner, parse, dcoref");
		StanfordCoreNLP pipeline = new StanfordCoreNLP(props);
		File inputFile = new File("D:\\Vishal\\corenlp-examples\\src\\test\\resources\\sample-content.txt");
		String text = Files.toString(inputFile, Charset.forName("UTF-8"));
		text = "Mansour plans to form a panel within fifteen days to review and suggest changes to the now-suspended constitution. Those amendments would be voted on in a  referendum within four months. Parliamentary elections would then be held,  perhaps in early 2014, followed by presidential elections upon the forming of a new parliament.\"\"";
		
		Annotation document = new Annotation(text);
		pipeline.annotate(document);

		List<CoreMap> sentences = document.get(SentencesAnnotation.class);
		for (CoreMap sentence : sentences) {
			for (CoreLabel token : sentence.get(TokensAnnotation.class)) {
				String word = token.get(TextAnnotation.class);
				String pos = token.get(PartOfSpeechAnnotation.class);

			}
		}
		Map<Integer, CorefChain> graph = document.get(CorefChainAnnotation.class);*/
		
		Sentence sent = new Sentence("Lucy is in the sky with diamonds and he lives in Ajitmal.");
		List<String> nerTags = sent.nerTags();
		List<String> lemmas = sent.lemmas();
		
	}

}
