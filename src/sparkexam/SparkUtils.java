package sparkexam;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

public class SparkUtils {
	public static JavaSparkContext getSparkContext(String name) {
		SparkConf conf = new SparkConf().setAppName(name).setMaster("local");
		return new JavaSparkContext(conf);
	}
}
