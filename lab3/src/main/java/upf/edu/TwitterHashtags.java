package upf.edu;

import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.twitter.TwitterUtils;
import twitter4j.Status;
import twitter4j.auth.OAuthAuthorization;
import upf.edu.util.ConfigUtils;
import upf.edu.storage.DynamoHashTagRepository;
import java.io.IOException;
/*This function writes into the DynamoDB database all the hashtags receive in streaming*/
public class TwitterHashtags {

    public static void main(String[] args) throws InterruptedException, IOException {
        String propertiesFile = args[0];
        OAuthAuthorization auth = ConfigUtils.getAuthorizationFromFileProperties(propertiesFile);

        SparkConf conf = new SparkConf().setAppName("Real-time Twitter Example");
        JavaStreamingContext jsc = new JavaStreamingContext(conf, Durations.seconds(5));
        // This is needed by spark to write down temporary data
        jsc.checkpoint("/tmp/checkpoint");

        final JavaReceiverInputDStream<Status> stream = TwitterUtils.createStream(jsc, auth);
        /*Creating the repository and storing all the received data into the database*/
        DynamoHashTagRepository my_db = new DynamoHashTagRepository();
        stream.foreachRDD(s->s.foreach(t->my_db.write(t)));
        stream.count().print();
        // Start the application and wait for termination signal
        jsc.start();
        jsc.awaitTermination();
    }
}
