package upf.edu.util;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import scala.Tuple2;

import java.util.List;

public class LanguageMapUtils {

    public static JavaPairRDD<String, String> buildLanguageMap(JavaRDD<String> lines) {
        /*Class to read the provided tsv. It separates the words of each rows. Then, it filters out those rows that
        * do not fits with the query. After that, we get the short version of the language, and the full language name*/
        JavaPairRDD<String,String> languages = lines.map(x->x.split("\t"))
                .filter(t->t[1].length()>1).filter(h->h[1].length()<3)
                .mapToPair(y-> new Tuple2<>(y[1], y[2])).distinct();
        return languages;
    }

}
