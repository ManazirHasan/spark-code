import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.sources.In;
import scala.Tuple2;

import java.util.*;

public class SparkTest {


    public static void main(String[] args) {
        try {


            System.out.println("9999999999");
            System.out.println("-----Test Spark-------");

            SparkSession ss = SparkSession.builder().appName("Spark-Test").master("local[2]").getOrCreate();
            Dataset<Row> dataset = ss.read().option("header","true").csv("D:\\codes\\sample-csv-file-for-testing.csv");
            dataset.createTempView("file");
            //dataset.cache();

            Dataset<Row> dfMontana= ss.sql("select * from file where Product=' Montana ' ");
            JavaRDD<String> test = dfMontana.toJavaRDD().map(x-> {
                String country = x.getAs("Country");
                String product = x.getAs("Product");
                String band = x.getAs("Discount Band ");
                return country+"---"+product+"---"+band;
            });
            test.collect().forEach(System.out::println);
            dfMontana.explain("extended");
            //dfMontana.show();
            //dataset.show(10);
            //dataset.take(10);
            //dataset.collect();
            //RDD vs DataFrame vs Dataset
            //Create SparkContext
        Dataset<String> dataset2 = ss.read().textFile("D:\\codes\\word-count.txt");
        JavaRDD<String> jrdd = dataset2.toJavaRDD().flatMap(x -> Arrays.asList(x.split(" ")).iterator());
        JavaPairRDD<String, Integer> mapToPair2=jrdd.mapToPair(w->new Tuple2<>(w,1));
        JavaPairRDD<String, Integer> mapToPair = jrdd.mapToPair(w -> new Tuple2<>(w, 1));
        /**Reduce the pair with key and add count*/
        JavaPairRDD<String, Integer> reduceByKey = mapToPair.reduceByKey((wc1, wc2) -> wc1 + wc2);
        System.out.println(reduceByKey.collectAsMap());

        List<Integer> ar = new ArrayList<Integer>();
        Collections.sort(ar, Comparator.reverseOrder());



        }catch (Exception ex){
            ex.printStackTrace();
        }
    }
}
