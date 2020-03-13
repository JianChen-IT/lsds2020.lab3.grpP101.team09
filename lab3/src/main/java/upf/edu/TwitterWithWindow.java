package upf.edu;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.twitter.TwitterUtils;
import scala.Tuple2;
import twitter4j.Status;
import twitter4j.auth.OAuthAuthorization;
import upf.edu.util.ConfigUtils;
import upf.edu.util.LanguageMapUtils;

import java.io.IOException;

public class TwitterWithWindow {
    public static void main(String[] args) throws IOException, InterruptedException {
        String propertiesFile = args[0]; //tweet.txt (credentials)
        String input = args[1]; //map.tsv
        OAuthAuthorization auth = ConfigUtils.getAuthorizationFromFileProperties(propertiesFile);

        SparkConf conf = new SparkConf().setAppName("Real-time Twitter with windows");
        JavaStreamingContext jsc = new JavaStreamingContext(conf, Durations.seconds(20));
        jsc.checkpoint("/tmp/checkpoint");

        final JavaReceiverInputDStream<Status> stream = TwitterUtils.createStream(jsc, auth);

        // Read the language map file as RDD
        final JavaRDD<String> languageMapLines = jsc
                .sparkContext()
                .textFile(input);
        final JavaPairRDD<String, String> languageMap = LanguageMapUtils
                .buildLanguageMap(languageMapLines);

        // create an initial stream that counts language within the batch (as in the previous exercise)
        final JavaPairDStream<String, Integer> languageCountStream = stream.mapToPair(x -> new Tuple2<>(x.getLang(),1))
                .transformToPair(t->t.join(languageMap))
                .mapToPair(y-> new Tuple2<String, Integer>(y._2._2, y._2._1))
                .reduceByKey((a,b) -> a + b); // IMPLEMENT ME

        // Prepare output within the batch
        final JavaPairDStream<Integer, String> languageBatchByCount = languageCountStream
                .mapToPair(w-> new Tuple2<>(w._2,w._1)).transformToPair(x->x.sortByKey(false)); // IMPLEMENT ME

        // Prepare output within the window
        final JavaPairDStream<Integer, String> languageWindowByCount = languageBatchByCount.window(Durations.seconds(300))
                .mapToPair(y-> new Tuple2<>(y._2,y._1))
                .reduceByKey((a,b)->a+b)
                .mapToPair(z-> new Tuple2<>(z._2,z._1))
                .transformToPair(a->a.sortByKey(false)); // IMPLEMENT ME

        // Print first 15 results for each one
        languageBatchByCount.print();
        languageWindowByCount.print();
        // Start the application and wait for termination signal
        jsc.start();
        jsc.awaitTermination();
    }
}
