package lemma;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import util.StringIntegerList;
import edu.umd.cloud9.collection.wikipedia.WikipediaPage;
import lemma.Tokenizer;

/**
 * Author: Elizabeth
 * Creates an index of lemma frequencies for each article.
 * This definitely doesn't work yet but I think it's a good outline.
 * I just don't know what the offset is for.
 */
public class LemmaIndexMapred {
	public static class LemmaIndexMapper extends Mapper<LongWritable, WikipediaPage, Text, StringIntegerList> {

		@Override
		public void map(LongWritable offset, WikipediaPage page, Context context) throws IOException,
				InterruptedException {
			// Get content of page
			String title = page.getTitle();
			String text = page.getWikiMarkup();
			// Tokenize? Or does this go somewhere else?
			Tokenizer tokenizer = new Tokenizer();
			List<String> lemmas = tokenizer.tokenize(text);
			// Count lemmas
			// Do I just go through every lemma manually or is there a better way to do this?
			// Write output
			// Not sure how to do this to get the correct format.
		}
	}
}
