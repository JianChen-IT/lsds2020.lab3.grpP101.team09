package upf.edu;
import upf.edu.storage.DynamoHashTagRepository;
import upf.edu.model.HashTagCount;

import java.util.List;

public class TwitterHashtagsReader {
    public static void main(String[] args){
        String language = args[0];
        DynamoHashTagRepository my_db = new DynamoHashTagRepository();
        List<HashTagCount> most_used_hashtags = my_db.readTop10(language);
        for(int i = 0; i < most_used_hashtags.size();i++){

            System.out.println("hashtag: " + most_used_hashtags.get(i).get_hashtag() + " count: " + most_used_hashtags.get(i).get_count());
        }
    }
}
