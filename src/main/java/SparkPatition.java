import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.rdd.RDD;
import org.apache.spark.sql.*;
import scala.collection.Iterator;
import scala.collection.immutable.Seq;
import scala.runtime.java8.JFunction0$mcB$sp;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class SparkPatition {
    public static void main(String[] args) {
        System.out.println("--------How to do partion---------");

        SparkConf sf = new SparkConf();
        //sf.set("spark.executor.cores","1");
        //sf.set("spark.num.executors","2");
        sf.setMaster("spark://localhost:7077");
        sf.setAppName("local-spark");
        //471859200 Bytes = 450 MB (in binary) executor memory can't lower then this
       // sf.set("spark.sql.files.maxPartitionBytes","134217728"); //default 128 mb =43 partion, 14 sec(if 512mb exec ram)
        //sf.set("spark.sql.files.maxPartitionBytes","134217728"); //default 128 mb =43 partion, 14 sec(if 512mb exec ram)
        //sf.set("spark.sql.files.maxPartitionBytes","134217728"); //default 128 mb =43 partion, 11 sec(if 2,3gb exec ram)

        //sf.set("spark.sql.files.maxPartitionBytes","268435456"); //256 mb , //3.1/.25= 12 * 2 = 24 part, 24 task ~ 16 sec
        //sf.set("spark.sql.files.maxPartitionBytes","536870912"); //512mb , //3.1/.5= 6 * 2 = 12 partis, 12 task ~ 13 sec
        sf.set("spark.sql.files.maxPartitionBytes","1073741824"); //1gb //3.1/1 = (3) * 2=6  partitions,6 task~13 sec
        //sf.set("spark.sql.files.maxPartitionBytes","268435456"); //default
        //sf.set("spark.sql.files.maxPartitionBytes","2147483648"); //2gb //3.1/2= 1.5 * 2 ~ 4 part, 4 task ~ 13 sec
        SparkSession ss = SparkSession.builder().config(sf).getOrCreate();
        Dataset<Row> dataset = ss.read().text("file:///home/admin/data/*.txt");
        //dataset= dataset.repartition(6);
        JavaRDD javaRdd = dataset.toJavaRDD();
        System.out.println("-----------------");
        System.out.println(javaRdd.getNumPartitions());
        System.out.println("-----------------");
        //javaRdd=javaRdd.coalesce(8);
        System.out.println("----------cccccc-------");
        System.out.println(javaRdd.getNumPartitions());
        System.out.println("--------cccccc---------");
        //javaRdd.cache();
        System.out.println("-----------------");
        System.out.println(ss.sparkContext().executorMemory());
        System.out.println("---------count------------");
        System.out.println(javaRdd.count());
        System.out.println("----------count-----------");
        ScheduledExecutorService service = Executors.newScheduledThreadPool(1);
        service.schedule(() -> ss.stop(), 5, TimeUnit.MINUTES);
        service.shutdown();
        System.out.println("---------------Exit-----------");


    }
}
