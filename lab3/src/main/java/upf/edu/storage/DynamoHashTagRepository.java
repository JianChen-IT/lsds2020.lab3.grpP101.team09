package upf.edu.storage;

import com.amazonaws.AmazonClientException;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.model.*;
import twitter4j.Status;
import upf.edu.model.HashTagCount;
import java.io.Serializable;
import java.util.*;
import java.util.stream.Collectors;

public class DynamoHashTagRepository implements IHashtagRepository, Serializable {

  static AmazonDynamoDB dynamoDB;
  @Override
  public void write(Status tweet) {
  /*
    ----------------LOGIN-----------------------
   */
    final String region = "us-east-1";
    final String output_table = "LSDS2020-TwitterHashtags";
    ProfileCredentialsProvider credentialsProvider = new ProfileCredentialsProvider();
    try {
      credentialsProvider.getCredentials();
    } catch (Exception e) {
      throw new AmazonClientException(
              "Cannot load the credentials from the credential profiles file. " +
                      "Please make sure that your credentials file is at the correct " +
                      "location (/home/carlos/.aws/credentials), and is in valid format.",
              e);
    }
    dynamoDB = AmazonDynamoDBClientBuilder.standard()
            .withCredentials(credentialsProvider)
            .withRegion(region)
            .build();
    //----------------------------------------------
    //----------------------WRITING-----------------
    String[] text = tweet.getText().split(" ");
    for(int i = 0; i < text.length;i++) {
      /*Conditional retrieve the hashtags from the text*/
      if (text[i].startsWith("#")) {
        if(tweet.getLang()!=null) {
          /*Putting or updating the information of the tweet that uses a determined hashtag.
          * As required, we safe the language, a counter and the tweet that uses the hashtag.
          * We safe the tweet ID instead of the whole tweet.*/
          Map<String,AttributeValue> key =  new HashMap<>();
          key.put("hashtag", new AttributeValue(text[i]));

          UpdateItemRequest putItemRequest = new UpdateItemRequest()
                  .withTableName(output_table)
                  .withKey(key)
                  .addAttributeUpdatesEntry("lang", new AttributeValueUpdate()
                          .withValue(new AttributeValue().withS(tweet.getLang())).withAction(AttributeAction.PUT))
                  .addAttributeUpdatesEntry("accumulator", new AttributeValueUpdate()
                          .withValue(new AttributeValue().withN("1")).withAction(AttributeAction.ADD))
                  .addAttributeUpdatesEntry("tweets", new AttributeValueUpdate()
                          .withValue(new AttributeValue().withSS(String.valueOf(tweet.getId()))).withAction(AttributeAction.ADD));
          UpdateItemResult putItemResult = dynamoDB.updateItem(putItemRequest);
        }
        else{
          continue;
        }
      }
    }
  }

  @Override
  public List<HashTagCount> readTop10(String lang) {
      /*
    ----------------LOGIN-----------------------
   */
    final String region = "us-east-1";
    final String output_table = "LSDS2020-TwitterHashtags";
    ProfileCredentialsProvider credentialsProvider = new ProfileCredentialsProvider();
    try {
      credentialsProvider.getCredentials();
    } catch (Exception e) {
      throw new AmazonClientException(
              "Cannot load the credentials from the credential profiles file. " +
                      "Please make sure that your credentials file is at the correct " +
                      "location (/home/carlos/.aws/credentials), and is in valid format.",
              e);
    }
    dynamoDB = AmazonDynamoDBClientBuilder.standard()
            .withCredentials(credentialsProvider)
            .withRegion(region)
            .build();
    //----------------------------------------------

    /*Getting all the records of the database*/
    ScanRequest scanRequest = new ScanRequest()
            .withTableName(output_table);

    ScanResult result = dynamoDB.scan(scanRequest);
    List <Map<String,AttributeValue>> all_hashtags = result.getItems();
    List <HashTagCount> hashtags_counts = new ArrayList<>();
    /*For loop to store only the hashtags used in the tweets of a given input language*/
    for (Map<String, AttributeValue> item : all_hashtags){
      if(lang.equals(item.get("lang").getS())) {
        hashtags_counts.add(new HashTagCount(item.get("hashtag").getS(), item.get("lang").getS(), Long.valueOf(item.get("accumulator").getN())));
      }
    }
    hashtags_counts.sort(Comparator.comparing(HashTagCount::get_count).reversed());


    return hashtags_counts.stream().limit(10).collect(Collectors.toList());
  }


}
