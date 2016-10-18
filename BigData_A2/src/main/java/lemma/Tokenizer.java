package lemma;

import java.util.List;
import java.util.Vector;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class Tokenizer {

	public Tokenizer() {
		// TODO Auto-generated constructor stubA
		//not used right now
	}

	public List<String> tokenize(String sentence) {
		// TODO implement your tokenizing code here
		//assuming all these items are already split by sentence
		
		//split on whitespace first
		String[] whitespaceSplit = sentence.split("\\s+");
		
		for(int i = 0; i < whitespaceSplit.length; i++){
			//remove important special characters
			if(removeToken(whitespaceSplit[i])){
				
			}
			//then remove them
		}
		//split on special characters
		//note: this will overgenerate but is the best solution for right now
		
		
		
		return null;
	}
	public Vector<String> fileCleaner(String token){
		String quoteTag = "\\&quot;(.+)\\&quot;";
		String linkTag = "{{(.+)}}";
		Vector<String> finalTokens = new Vector<String>();
		if(token.contains(quoteTag)){
			String[] quotesTokens = token.split("\\&quot;");
			finalTokens.add(quotesTokens[0]);
		}
		else if(token.contains(linkTag)){
			//first remove markers
			String newToken = token.replace("{{", "");
			newToken = token.replace("{{", "");
			
			//now split on |
			String[] linksTokens = token.split("\\|");
			for (int i = 0; i < linksTokens.length; i++){
				if(i%2 == 0){//want the even numbers
					finalTokens.add(linksTokens[i]);
				}
			}
		}
		else{
			//got all complex categories, now split by simple chars:
			String[] specialCharSplit = token.split("\\W");
			Pattern pattern = Pattern.compile("\\W");
			for(int i = 0; i < specialCharSplit.length; i++){
				Matcher matcher = pattern.matcher(specialCharSplit[i]);
				
			}
		}
		return finalTokens;
	}
	private boolean removeToken(String token){
		String fileTag = "[[File:(.+)]]";
		String refTag = ",&lt;(.+)";
		if(token.contains(fileTag)){
			return true;
		}
		return false;
	}
	
	private String lemmatizer(String token){
		return null;
	}
		
}

