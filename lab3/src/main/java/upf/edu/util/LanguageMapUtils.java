package upf.edu.util;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import scala.Tuple2;

import java.util.List;

public class LanguageMapUtils {

    public static JavaPairRDD<String, String> buildLanguageMap(JavaRDD<String> lines) {

        JavaPairRDD<String,String> languages = lines.map(x->x.split("\t"))
                .filter(t->t[1].length()>1).filter(h->h[1].length()<3)
                .mapToPair(y-> new Tuple2<String,String>(y[1], y[2])).distinct();
        //System.out.println(languages.take(5));

        /*List<String[]> heu = languages.collect();
        String[] ug = heu.get(3);
        System.out.println("TESTIIIIIIIIIIING222222222:  " + ug[0].toString()+"  "+ ug[1].toString()+"  "+ ug[2].toString()+"  "+ ug[3].toString()+"  "+ ug[4].toString());
        System.out.println("TESTIIIIIIIIIIING7777777777:  " + (ug[0].length()==3));
        System.out.println("TESTIIIIIIIIIIING2feqewfihiwofw:  " + (ug[1].length()<1));*/

        /*List<String> heu = lines.collect();
        String ef = heu.get(3);
        System.out.println("TESTIIIIIIIIIIING:  " + ef.split("\t").length);
        String[] ug = ef.split("\t");
        System.out.println("TESTIIIIIIIIIIING222222222:  " + ug[0].toString()+"  "+ ug[1].toString()+"  "+ ug[2].toString()+"  "+ ug[3].toString()+"  "+ ug[4].toString());
        System.out.println("TESTIIIIIIIIIIING7777777777:  " + (ug[0].length()==3));
        System.out.println("TESTIIIIIIIIIIING2feqewfihiwofw:  " + (ug[1].length()<1));*/

        return languages;// IMPLEMENT ME
    }

}
