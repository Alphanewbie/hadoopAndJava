package sparkexam;

import java.util.Arrays;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;

public class WordCount8 {

	public static void main(String[] args) throws Exception {
		SparkConf conf = new SparkConf().setAppName("WordCount_version8").setMaster("local");
	
		JavaSparkContext sc = new JavaSparkContext(conf);

		JavaRDD<String> rdd1 = sc.textFile("hdfs://192.168.111.120:9000/edudata/fruits.txt");
	

		JavaRDD<String> rdd2 = rdd1.flatMap(line -> Arrays.asList(line.split("[\\s]+")).iterator());

		JavaPairRDD<String, Long> rdd3 = rdd2.mapToPair(w -> new Tuple2<String, Long>(w, 1L));

		JavaPairRDD<String, Long> rdd4 = rdd3.reduceByKey((x, y) -> x + y);
		
		rdd4.saveAsTextFile("hdfs://192.168.111.120:9000/result/sparkoutput12");
		Thread.sleep(50000);
		sc.close();

	}

}
