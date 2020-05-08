package sparkexam;

import java.util.Arrays;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class SparkLab1 {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		JavaSparkContext sc = SparkUtils.getSparkContext("test9");
		
		doclickcount(sc);
		
		sc.stop();
	}
	
	public static void doclickcount(JavaSparkContext sc) {
		
		JavaRDD<String,String> rdd1 = sc.textFile("hdfs://192.168.111.120:9000/edudata/product_click.log");
		
		JavaRDD<String> rdd2 = rdd1.flatMap((String line) -> Arrays.asList(line.split("[\\s]+")).iterator());
		
		
		JavaPairRDD<String, Integer> rdd3 = rdd2.mapToPair(word -> new Tuple2<String, Integer>(word, 1));
		
		JavaPairRDD<String, Integer> rdd4 = rdd3.reduceByKey((a, b) -> a + b);		
		JavaPairRDD<String, Integer> rdd5 = rdd4.sortByKey( );
		rdd6.foreach(tupledata-> System.out.println("("+tupledata._1+","+tupledata._2+")"));
		
	}

}
