package upf.edu;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.twitter.TwitterUtils;
import twitter4j.Status;
import twitter4j.auth.OAuthAuthorization;
import upf.edu.util.ConfigUtils;
import upf.edu.util.LanguageMapUtils;

import java.io.IOException;


public class Pruebas {

    public static void main(String[] args) throws InterruptedException, IOException {
        String map = args[0];
        System.out.println(map);
        SparkConf conf = new SparkConf().setAppName("testing");
        JavaSparkContext sparkContext = new JavaSparkContext(conf);
        JavaRDD<String> input = sparkContext.textFile(map);
        LanguageMapUtils.buildLanguageMap(input);

    }


}
