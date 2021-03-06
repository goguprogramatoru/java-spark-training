package wordcount;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintWriter;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.Arrays;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import scala.Tuple2;

import org.apache.log4j.Logger;
import org.apache.log4j.Level;

/**
 *
 * @author radu
 */
public class Main {

	private static final String STORY_URL = "http://textfiles.com/stories/3lpigs.txt";
	private static final String STORY_FILE_PATH = "/home/gameloft/Documents/sparkTraining/";
	private static final String STORY_FILE = STORY_FILE_PATH + "story.txt";

	public static void main(String[] args) {
		// reduce spark logging (too much noise)
		Logger.getLogger("org").setLevel(Level.WARN);
		Logger.getLogger("akka").setLevel(Level.WARN);
		
		// make sure paths exist; create if not
		try {
			FileUtils.forceMkdir(FileUtils.getFile(STORY_FILE_PATH));
		} catch (IOException ex) {
			System.err.println(ex.getMessage());
		}

		// because methods are not static...
		Main p = new Main();

		// download file to use for word count
		p.downloadStory();

		// run spark word count
		p.sparkWordCount();
	}

	private void downloadStory() {
		try {
			URL url = new URL(STORY_URL);
			InputStream input = url.openStream();
			PrintWriter pw = new PrintWriter(new File(STORY_FILE));
			pw.write(IOUtils.toString(input));
			pw.close();
		} catch (FileNotFoundException ex) {
			System.err.println(ex.getMessage());
		} catch (MalformedURLException ex) {
			System.err.println(ex.getMessage());
		} catch (IOException ex) {
			System.err.println(ex.getMessage());
		}
	}

	private void sparkWordCount() {
		// create a spark config object
		SparkConf conf = new SparkConf().setAppName("Spark Word Count").setMaster("local[*]");
		
		// create a spark context with the previously defined config
		JavaSparkContext sc = new JavaSparkContext(conf);

		// get all the lines from text file as a RDD
		JavaRDD<String> lines = sc.textFile(STORY_FILE);

		// process the lines RDD to another RDD containing the words
		// filter words of zero length
		JavaRDD<String> words = lines.map(line -> line.replaceAll("[^A-Za-z0-9 ]", " "))
				.flatMap((String t) -> Arrays.asList(t.split(" ")).iterator())
				.filter(word -> !word.equals(""));

		// get ready for counting: transform words RDD into a pair RDD
		JavaPairRDD<String, Integer> pairs = words.mapToPair(s -> new Tuple2(s, 1));

		// do the actual counting, reducing the words by their key and accumulating the values
		JavaPairRDD<String, Integer> counts = pairs.reduceByKey((a, b) -> a + b);

		// invert keys and values to be able to sort by key
		JavaPairRDD<Integer, String> invertedCounts = counts.mapToPair(tpl -> new Tuple2(tpl._2, tpl._1));

		// get another pair RDD with counts and words, where counts are the keys
		JavaPairRDD<Integer, String> sortedCounts = invertedCounts.sortByKey(false);

		// print out the results
		sortedCounts.foreach(tpl -> System.out.println(tpl._1 + " - " + tpl._2));

	}

}
