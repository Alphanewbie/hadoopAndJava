package mrexam;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

// word 카운트
public class WordCountReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
	private IntWritable result = new IntWritable();

	public void reduce(Text key, Iterable<IntWritable> values, Context context)
			throws IOException, InterruptedException {
		int sum = 0;
		// 값을 받아온다.
		System.out.println("ReDuce: " + key + "~~~");
		for (IntWritable val : values) {
			int data = val.get();
			System.out.println(data);
			sum += data;
		}
		result.set(sum);
		context.write(key, result);
	}
}