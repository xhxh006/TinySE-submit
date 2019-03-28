package edu.hanyang.submit;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import edu.hanyang.indexer.Tokenizer;

import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.core.SimpleAnalyzer;
import org.tartarus.snowball.ext.PorterStemmer;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;

public class TinySETokenizer implements Tokenizer {
	SimpleAnalyzer simpleanalyzer;
	PorterStemmer porterstemmer;

	public void setup() {
	
		simpleanalyzer = new SimpleAnalyzer();
		porterstemmer = new PorterStemmer(); 
		
	}

	public List<String> split(String text) {
		List<String> result = new ArrayList<String>();
		TokenStream tokenStream = simpleanalyzer.tokenStream("fieldName", text);
		CharTermAttribute charTermAttribute = tokenStream.addAttribute(CharTermAttribute.class);
		
		try {
			tokenStream.reset();
			while(tokenStream.incrementToken()) {
				porterstemmer.setCurrent(charTermAttribute.toString());
				porterstemmer.stem();
				result.add(porterstemmer.getCurrent());
				
			}
			tokenStream.close();
		} catch (IOException e) {
			
			e.printStackTrace();
			
		}
				
		return result;
	}

	public void clean() {
		
		simpleanalyzer.close();
		
	}

}
