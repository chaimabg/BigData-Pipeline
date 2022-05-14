package tn.insat.Project;

import java.io.IOException;
import java.util.Random;
import java.util.*;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;



public class Reducer extends MapReduceBase implements Reducer<Text, IntWritable, Text, Text> {

	public void reduce(Text t_key, Iterator<IntWritable> values, OutputCollector<Text,Text> output, Reporter reporter) throws IOException {
		Text key = t_key;
		int casesForState = 0;
		while (values.hasNext()) {
			// replace type of value with the actual type of our value
			IntWritable value = (IntWritable) values.next();
			casesForState += value.get();
			
		}
		
		
		output.collect(new Text(key), new Text(Integer.toString(casesForState)));
	}
}