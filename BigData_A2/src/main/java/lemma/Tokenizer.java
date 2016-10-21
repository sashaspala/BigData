package lemma;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.json.JSONException;
import org.json.JSONObject;

import com.mashape.unirest.http.HttpResponse;
import com.mashape.unirest.http.JsonNode;
import com.mashape.unirest.http.Unirest;
import com.mashape.unirest.http.exceptions.UnirestException;


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
		ArrayList<String> tokens = new ArrayList<String>();
		ArrayList<String> finalTokens = new ArrayList<String>();
		String[] sentences = sentenceTokenizer(content);
		
		// TODO implement your tokenizing code here
		//assuming all these items are already split by sentence
		
		//split on whitespace first
		for(int i = 0; i < sentences.length; i++){
			String[] whitespaceSplit = sentences[i].split("\\s+");
				//remove important special characters
			for(int j = 0; j < whitespaceSplit.length; j++){
				if(!removeToken(whitespaceSplit[j])){
					//add to finalTokens
					if(!whitespaceSplit[j].equals("\\s")){
						ArrayList<String> cleanedTokens = fileCleaner(whitespaceSplit[j]);
						tokens.addAll(cleanedTokens);
					}
				}
			}
		}
		//note: this will overgenerate but is the best solution for right now
		
		//now normalize and remove stopwords
		finalTokens = normalizer(tokens);
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
			//got all complex categories, now split by simple special chars:
			String[] specialCharSplit = token.split("\\W+");
			Pattern pattern = Pattern.compile("\\W+");
			for(int i = 0; i < specialCharSplit.length; i++){
				Matcher matcher = pattern.matcher(specialCharSplit[i]);
				if (!matcher.find()){
					finalTokens.add(specialCharSplit[i]);
				}
			}
		}
		return finalTokens;
	}
	private boolean removeToken(String token){
		String fileTag = "\\[\\[File:(.+)";
		String refTag = ",&lt;{1}(.+)";
		String numeralTag = "\\d+";
		Pattern fPattern = Pattern.compile(fileTag);
		Pattern rPattern = Pattern.compile(refTag);
		Pattern nPattern = Pattern.compile(numeralTag);
		
		Matcher fMatcher = fPattern.matcher(token);
		Matcher rMatcher = rPattern.matcher(token);
		Matcher nMatcher = nPattern.matcher(token);
		if(fMatcher.find() || rMatcher.find() || nMatcher.find()){
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
		ArrayList<String> junkwords = new ArrayList<String>(Arrays.asList("wikipedia", "namespace", "http", "org", 
				"com", "main_page", "mediawiki", "namespaces", "xml", "lang", "en", "siteinfo", "sitename", "deadurl", "gt",
				"dashbot", "archivedate", "archiveurl", "accessdate"));
		for(int i = 0; i < tokens.size(); i++){
			//to lowercase
			String temp = tokens.get(i).substring(0, tokens.get(i).length()).toLowerCase();
			
			//call lemmatizer here
			if (!stopwords.contains(temp) & !junkwords.contains(temp) && !temp.isEmpty()){
				try {
					String finalLemma = lemmatizer(temp);
					normalizedStrings.add(finalLemma);
				} catch (UnirestException e){
					e.printStackTrace();
						
				} catch (JSONException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		}
		return normalizedStrings;
	}
	
	private String lemmatizer(String token) throws UnirestException, JSONException{
		//using twinword hosted lemmatizer at https://market.mashape.com/twinword/lemmatizer-free
		/*HttpResponse<JsonNode> response = Unirest.post("https://twinword-lemmatizer1.p.mashape.com/extract/")
				.header("X-Mashape-Key", "hD7gJXJlSnmshMf5WnWoqqs9ACnhp1X1WvYjsnJ3Pty5qiGv0H")
				.header("Content-Type", "application/x-www-form-urlencoded")
				.header("Accept", "application/json")
				.field("text", token)
				.asJson();
		
		System.out.println("completed post");
		
		//deal with the returned jsonnode
		//this is unnecessarily complicated....
		JSONObject lemmaObject = response.getBody().getObject();
		//String lemma = lemmaObject.getString("lemma");
		 * 
		 This runs too slowly :( :( :( :(*/
		//Pattern pattern = Pattern.compile("(ize\\Z)|(ed\\Z)|(ing\\Z)|s\\Z");
		Pattern pattern = Pattern.compile("ed\\Z|(ing)\\Z|s\\Z");
		Matcher matcher = pattern.matcher(token);
		if(matcher.find()){
			//System.out.println("token: " + token);
			token = token.substring(0,matcher.start());
			if(!matcher.group().equals("s")){
				Pattern cPattern = Pattern.compile("[aeiou][^aeiou]\\Z");
				Matcher consonantMatcher = cPattern.matcher(token);
				if(consonantMatcher.find()){
					//ends with a VC (shar from shared, retriev from retrieved)
					token = token.concat("e");
				}
			}
			
		}
		return token;
	}

	/*public static void main(String[] args){
		//sorry for chunks of nothing here:
		String totalContent = "";
		try {
			BufferedReader br = new BufferedReader(new FileReader("wiki_example.txt"));
			
			String line;
			while((line = br.readLine()) != null){
				totalContent = totalContent.concat(line);
			}
			br.close();
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		//send content to tokenizer
		Tokenizer tokenizer = new Tokenizer();
		tokenizer.tokenize(totalContent);
	}
	*/
}


