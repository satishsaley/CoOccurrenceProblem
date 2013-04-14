package com.satishproj2.hadoop;

import java.io.IOException;
import java.util.HashMap;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

public class CoOccurPairMap extends Mapper<LongWritable, Text, Pair, IntWritable> {

	private final static IntWritable one = new IntWritable(1);
	private Text word = new Text();

	public final static String[] listOfWords = new String[]{ "can't","u","i","&","just","rt","$$","-","ya","x","s","i'd","ur","said","did","hi","te","n","e","yo","having","1","2","3","4","5","6","7","8","9","0","la","em","o","es","en","lo","w","t","h", "fucking","bitch","a", "about", "above", "above",
		"across", "after", "afterwards", "again", "against", "all",
		"almost", "alone", "along", "already", "also", "although",
		"always", "am", "among", "amongst", "amoungst", "amount", "an",
		"and", "another", "any", "anyhow", "anyone", "anything", "anyway",
		"anywhere", "are", "around", "as", "at", "back", "be", "became",
		"because", "become", "becomes", "becoming", "been", "before",
		"beforehand", "behind", "being", "below", "beside", "besides",
		"between", "beyond", "both", "bottom", "but", "by", "call",
		"can", "cannot", "cant", "co", "com", "con", "could", "couldnt", "cry",
		"de", "describe", "detail", "do", "done", "down", "due", "during",
		"each", "eg", "eight", "either", "eleven", "else", "elsewhere",
		"empty", "enough", "etc", "even", "ever", "every", "everyone",
		"everything", "everywhere", "except", "few", "fifteen", "fify",
		"fill", "find", "fire", "first", "five", "for", "former",
		"formerly", "forty", "found", "four", "free", "from", "front", "full",
		"further", "get", "give", "go", "had", "has", "hasnt", "have",
		"he", "hence", "her", "here", "hereafter", "hereby", "herein",
		"hereupon", "hers", "herself", "him", "himself", "his", "how",
		"however", "hundred", "ie", "if", "in", "inc", "indeed",
		"interest", "into", "is", "it", "its", "itself", "keep", "last",
		"latter", "latterly", "least", "less", "ltd", "made", "many",
		"may", "me", "meanwhile", "might", "mill", "mine", "more",
		"moreover", "most", "mostly", "move", "much", "must", "my",
		"myself", "name", "namely", "neither", "net", "never", "nevertheless",
		"next", "nine", "no", "nobody", "none", "noone", "nor", "not",
		"nothing", "now", "nowhere", "of", "off", "often", "on", "once",
		"one", "only", "onto", "or", "org", "other", "others", "otherwise", "our",
		"ours", "ourselves", "out", "over", "own", "part", "per",
		"perhaps", "please", "put", "rather", "re", "same", "see", "seem",
		"seemed", "seeming", "seems", "serious", "several", "she",
		"should", "show", "side", "since", "sincere", "six", "sixty", "so",
		"some", "somehow", "someone", "something", "sometime", "sometimes",
		"somewhere", "still", "such", "system", "take", "ten", "than",
		"that", "the", "their", "them", "themselves", "then", "thence",
		"there", "thereafter", "thereby", "therefore", "therein",
		"thereupon", "these", "they", "thickv", "thin", "third", "this",
		"those", "though", "three", "through", "throughout", "thru",
		"thus", "to", "together", "too", "top", "toward", "towards",
		"twelve", "twenty", "two", "un", "under", "until", "up", "upon",
		"us", "very", "via", "was", "we", "well", "were", "what",
		"whatever", "when", "whence", "whenever", "where", "whereafter",
		"whereas", "whereby", "wherein", "whereupon", "wherever",
		"whether", "which", "while", "whither", "who", "whoever", "whole",
		"whom", "whose", "why", "will", "wikipedia", "with", "within", "without",
		"would", "yet", "you", "your", "yours", "yourself", "yourselves",
		"the","i","a" };
	
	@Override
	protected void cleanup(Context context) throws IOException,
	InterruptedException {
		// TODO Auto-generated method stub
		super.cleanup(context);
	}

	@Override
	protected void map(LongWritable key, Text value,
			Context context) throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		int count=0;
		String line = value.toString();
		HashMap< Integer,String> stopWords=new HashMap< Integer,String>();
		int lengthOfList=listOfWords.length;

		for(int i=1;i<lengthOfList;i++)
		{
			stopWords.put(new Integer(i),listOfWords[i]);
		}

		StringTokenizer tokenizer = new StringTokenizer(line);
		String line1=line;
		String innerLoopWord;
		String outerLoopWord;
		
		while (tokenizer.hasMoreTokens()) {
			
			outerLoopWord=tokenizer.nextToken().toLowerCase().replace("\"", "").trim();
			StringTokenizer stInnterTokenizer=new StringTokenizer(line1);
			while(stInnterTokenizer.hasMoreTokens())
			{
				innerLoopWord=stInnterTokenizer.nextToken().toLowerCase().replace("\"", "");
				if(stopWords.containsValue(new String(outerLoopWord.trim()))==false &&
						stopWords.containsValue(new String(innerLoopWord.trim()))==false
						)
				{
					Pair p=new Pair(new Text(outerLoopWord), new Text(innerLoopWord));
					context.write(p, one);
					Pair p2=new Pair(new Text(outerLoopWord),new Text("*"));
					context.write(p2, one);	
				}
				

			}//inner while
			
		}//outer while
	}//map method
}


