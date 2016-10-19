package lemma;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import util.StringIntegerList;
import edu.umd.cloud9.collection.wikipedia.WikipediaPage;

import java.util.HashMap

import lemma.Tokenizer;

/**
 * Author: Elizabeth
 * Creates an index of lemma frequencies for each article.
 */
public class LemmaIndexMapred {
	public static class LemmaIndexMapper extends Mapper<LongWritable, WikipediaPage, Text, StringIntegerList> {

		@Override
		public void map(LongWritable offset, WikipediaPage page, Context context) throws IOException,
				InterruptedException {
			// Get content of page - comes from WikiPage from GetArticlesMapred
			Text title = page.getTitle();
			String text = page.getWikiMarkup();
			// Tokenize
			tokenizer = new Tokenizer();
			lemmas = tokenizer.tokenize(text);
			// Count lemmas
			Map<String, Integer> freqs = new HashMap<String, Integer>;
			for (lemma : lemmas){
				if(freqs.containsKey(lemma)){
					freqs.put(lemma, freqs.get(lemma) + 1);
				} else {
					freqs.put(lemma, 1)
				}
			}
			StringIntegerList list = new StringIntegerList(freqs);
			// Write output
			context.write(title, list);
		}
	}
}
