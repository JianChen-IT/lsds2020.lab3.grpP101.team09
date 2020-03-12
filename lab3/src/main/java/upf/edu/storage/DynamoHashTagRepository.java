package upf.edu.storage;

import twitter4j.Status;
import upf.edu.model.HashTagCount;

import java.io.Serializable;
import java.util.*;

public class DynamoHashTagRepository implements IHashtagRepository, Serializable {

  @Override
  public void write(Status tweet) {

    final static String endpoint = "dynamodb.us-east-1.amazonaws.com";
    final static String region = "us-east-1";

    final AmazonDynamoDB client = AmazonDynamoDBClientBuilder.standard()
            .withEndpointConfiguration(
                    new AwsClientBuilder.EndpointConfiguration(endpoint, region)
            ).withCredentials(new ProfileCredentialsProvider("upf"))
            .build();
    final DynamoDB dynamoDB = new DynamoDB(client);
    final Table dynamoDBTable = dynamoDB.getTable();
  }

  @Override
  public List<HashTagCount> readTop10(String lang) {

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
