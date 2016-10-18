# BigData
Brandeis Big Data A2
Schuyler test
hadoop_2: Sasha Spala, Schuyler Rank, Elizabeth Wells, Meghan Hickey

GetArticlesMapred
Input= wikipedia dump file
Output= wikipediapage


LemmaIndexMapred
Input= Wikipedia page
Feeds sentences to tokenizer

Tokenizer
passes back list of string to LemmaIndexMapred

InvertedIndexMapred
Takes in ArticleID and text that’s the index from LemmaIndexMapred
