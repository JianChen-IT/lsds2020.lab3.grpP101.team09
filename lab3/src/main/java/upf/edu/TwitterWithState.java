package upf.edu;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.twitter.TwitterUtils;
import scala.Tuple2;
import twitter4j.Status;
import twitter4j.auth.OAuthAuthorization;
import upf.edu.util.ConfigUtils;

import org.apache.spark.api.java.Optional;

import java.io.IOException;
import java.util.List;


public class TwitterWithState {

     public static void main(String[] args) throws IOException, InterruptedException {
        String propertiesFile = args[0];
        String language = args[1];
        OAuthAuthorization auth = ConfigUtils.getAuthorizationFromFileProperties(propertiesFile);

        SparkConf conf = new SparkConf().setAppName("Real-time Twitter With State");
        JavaStreamingContext jsc = new JavaStreamingContext(conf, Durations.seconds(20));
        jsc.checkpoint("/tmp/checkpoint");

        final JavaReceiverInputDStream<Status> stream = TwitterUtils.createStream(jsc, auth);

        // create a simpler stream of <user, count> for the given language
        final JavaPairDStream<String, Integer> tweetPerUser = stream.filter(c->c.getLang().equals(language))
                .mapToPair(x-> new Tuple2<>(x.getUser().getScreenName(),1)).reduceByKey((a,b)->a+b)
                .transformToPair(l -> l.sortByKey(false));
        /*Function, used in the updateStateBykey, that counts the number of tweets associated to a given user.*/
        Function2<List<Integer>, Optional<Integer>, Optional<Integer>>
            updateFunction = (nums, current) -> {
                int sum = current.orElse(0);
                for (int i : nums) {
                    sum += i;
                }
                return Optional.of(sum);
        };

        /*transform to a stream of <userTotal, userName> and get the first 20. The stream will accumulate the results of
        * the different batches, and it will get the top 20 of the accumulated results.*/
        final JavaPairDStream<Integer,String> tweetsCountPerUser = tweetPerUser
                .updateStateByKey(updateFunction)
                .mapToPair(n-> new Tuple2<>(n._2, n._1))
                .transformToPair(k->k.sortByKey(false));

         tweetsCountPerUser.print(20);

        // Start the application and wait for termination signal
        jsc.start();
        jsc.awaitTermination();
    }
}
