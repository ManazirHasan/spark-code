import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.Iterator;

public class TestC {

    public static void main(String[] args) {

        //SparkContext sc = new SparkContext(new SparkConf().setMaster("local[3]").setAppName("chec"));
        //SparkSession ss = new SparkSession(sc);
        SparkSession ss = SparkSession.builder().appName("Spark-Test").master("local[1]").getOrCreate();
        System.out.println(ss);
        Dataset<Row> dataset = ss.read().load("");
        //dataset.toJavaRDD().mapPartitionsWithIndex({case (i,rows) =>Iterator(i,rows.size)})
        dataset.count();

    }
}
