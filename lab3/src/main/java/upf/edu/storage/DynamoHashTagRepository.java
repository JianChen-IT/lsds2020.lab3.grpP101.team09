package upf.edu.storage;

import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Table;
import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.twitter.TwitterUtils;
import twitter4j.Status;
import twitter4j.auth.OAuthAuthorization;
import upf.edu.model.HashTagCount;
import upf.edu.util.ConfigUtils;

import java.io.IOException;
import java.io.Serializable;
import java.util.*;

public class DynamoHashTagRepository implements IHashtagRepository, Serializable {

  @Override
  public void write(Status tweet) {

    final String endpoint = "dynamodb.us-east-1.amazonaws.com";
    final String region = "us-east-1";

    final AmazonDynamoDB client = AmazonDynamoDBClientBuilder.standard()
            .withEndpointConfiguration(
                    new AwsClientBuilder.EndpointConfiguration(endpoint, region)
            ).withCredentials(new ProfileCredentialsProvider("upf"))
            .build();
    final DynamoDB dynamoDB = new DynamoDB(client);
    //final Table dynamoDBTable = dynamoDB.getTable();
  }

  @Override
  public List<HashTagCount> readTop10(String lang) {
    return null;
  }

  public static void main(String[] args) throws InterruptedException, IOException {
    String propertiesFile = args[0];
    String language = args[1];

    OAuthAuthorization auth = ConfigUtils.getAuthorizationFromFileProperties(propertiesFile);

    SparkConf conf = new SparkConf().setAppName("Real-time Twitter Example");
    JavaStreamingContext jsc = new JavaStreamingContext(conf, Durations.seconds(10));

    // This is needed by spark to write down temporary data
    jsc.checkpoint("/tmp/checkpoint");

    final JavaReceiverInputDStream<Status> stream = TwitterUtils.createStream(jsc, auth);

    // Print the stream (only 5 tweets)
    stream.print(5);

    // Start the application and wait for termination signal
    jsc.start();
    jsc.awaitTermination();

  }

}
