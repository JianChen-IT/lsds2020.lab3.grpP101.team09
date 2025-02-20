package upf.edu.model;

import com.google.gson.Gson;

public final class HashTagCount {
  protected static Gson gson = new Gson();

  final String hashTag;
  final String lang;
  final Long count;

  public HashTagCount(String hashTag, String lang, Long count) {
    this.hashTag = hashTag;
    this.lang = lang;
    this.count = count;
  }
  /*Getters used for getting the top 10 hashtags*/
  public Long get_count(){
    return this.count;
  }
  public String get_hashtag(){
    return this.hashTag;
  }
  @Override
  public String toString() {
    return gson.toJson(this);
  }
}
