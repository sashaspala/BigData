package lemma;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class Tokenizer {
	

	public Tokenizer() {
		// TODO Auto-generated constructor stubA
		//not used right now
	}
	
	private String[] sentenceTokenizer(String content){
		//roughly split content by . , following whitespace, and capital letter:
		String[] sentences = content.split("\\.(\\s+)[A-Z]");
		return sentences;
	}

	public ArrayList<String> tokenize(String content) {
		ArrayList<String> finalTokens = new ArrayList<String>();
		String[] sentences = sentenceTokenizer(content);

		// TODO implement your tokenizing code here
		//assuming all these items are already split by sentence
		
		//split on whitespace first
		for(int i = 0; i < sentences.length; i++){
			String[] whitespaceSplit = sentences[i].split("\\s+");
			//cast toList to make life easier:
			for(int j = 0; j < whitespaceSplit.length; i++){
				//remove important special characters
				if(!removeToken(whitespaceSplit[i])){
					//add to finalTokens
					finalTokens.add(whitespaceSplit[i]);
				}
			}
		}
		//note: this will overgenerate but is the best solution for right now
		
		//now normalize and remove stopwords
		return finalTokens;
	}
	public ArrayList<String> fileCleaner(String token){
		String quoteTag = "\\&quot;(.+)\\&quot;";
		String linkTag = "{{(.+)}}";
		
		ArrayList<String> finalTokens = new ArrayList<String>();
		if(token.contains(quoteTag)){
			String[] quotesTokens = token.split("\\&quot;");
			finalTokens.add(quotesTokens[0]);
		}
		else if(token.contains(linkTag)){
			//first remove markers
			String newToken = token.replace("{{", "");
			newToken = newToken.replace("{{", "");
			
			//now split on |
			String[] linksTokens = newToken.split("\\|");
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
		String numeralTag = "\\d+";
		if(token.contains(fileTag) || token.contains(refTag) || token.contains(numeralTag)){
			return true;
		}
		return false;
	}
	private ArrayList<String> normalizer(List<String> tokens){
		ArrayList<String> normalizedStrings = new ArrayList<String>();
		ArrayList<String> stopwords = new ArrayList<String>(Arrays.asList("a","about","above","after","again","against","all","am","an",
				"and","any","are","aren't","as","at","be","because","been","before","being","below","between","both","but","by",
				"can't","cannot","could","couldn't","did","didn't","do","does","doesn't","doing","don't","down","during","each",
				"few","for","from","further","had","hadn't","has","hasn't","have","haven't","having","he","he'd","he'll","he's",
				"her","here","here's","hers","herself","him","himself","his","how","how's","i","i'd","i'll","i'm","i've","if",
				"in","into","is","isn't","it","it's","its","itself","let's","me","more","most","mustn't","my","myself","no",
				"nor","not","of","off","on","once","only","or","other","ought","our","ours","ourselves",
				"out","over","own","same","shan't","she","she'd","she'll","she's","should","shouldn't","so","some","such",
				"than","that","that's","the","their","theirs","them","themselves","then","there","there's","these","they",
				"they'd","they'll","they're","they've","this","those","through","to","too","under","until","up","very","was",
				"wasn't","we","we'd","we'll","we're","we've","were","weren't","what","what's","when","when's","where","where's",
				"which","while","who","who's","whom","why","why's","with","won't","would","wouldn't","you","you'd","you'll",
				"you're","you've","your","yours","yourself","yourselves"));
		for(int i = 0; i < tokens.size(); i++){
			//to lowercase
			String temp = tokens.get(i).substring(0, tokens.get(i).length()).toLowerCase();
			//call lemmatizer here
			
			
			if (!stopwords.contains(temp)){
				
			}
			
		}
		return normalizedStrings;
	}
	
	private String lemmatizer(String token){
		return null;
	}
		

	public static void main(String[] args){
		
	}
}


