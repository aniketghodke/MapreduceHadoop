/**
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

import java.io.IOException;
import java.util.HashMap;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class PerTaskTally {

	public static class TokenizerMapper extends
			Mapper<Object, Text, Text, IntWritable> {
		// Part 4 changes starts
		/* These changes are made since there is no need to emit just 1*/
		//private final static IntWritable one = new IntWritable(1);
		// Part 4 changes ends
		private Text word = new Text();
		// Part 4 changes starts
		/* This is a per task approach so defining a hash map to store the 
		 * count of each word instead of emitting 1 for each word we will emit 
		 * the entire count in just one go*/
		HashMap<String, Integer> storageHash;
		public void setup(Context context){
			storageHash = new HashMap<String,Integer>();
		}
		// Part 4 changes ends
		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {
			StringTokenizer itr = new StringTokenizer(value.toString());
			while (itr.hasMoreTokens()) {
				word.set(itr.nextToken());
				// Aniket changes starts
				/* These changes are made so that the map will emit only the required
		         * word set i.e. emit only "real" words*/
				String word_new = word.toString();
				char start = word_new.charAt(0);
				if (start == 'm' || start == 'M' || start == 'n'
						|| start == 'N' || start == 'o' || start == 'O'
						|| start == 'p' || start == 'P' || start == 'q'
						|| start == 'Q') {
					// Part 4 changes starts
					/* removing the emit just 1 step*/
					//context.write(word, one);
					if(storageHash.containsKey(word_new)){
						Integer value_present = storageHash.get(word_new);
						storageHash.put(word_new, value_present + 1);
					}else{
						storageHash.put(word_new, 1);
					}
					// Part 4 changes ends
				}
				// Aniket changes ends
			}
			
		}
		public void cleanup(Context context) throws IOException, InterruptedException{
			// Part4 changes starts
			/* Instead of emitting 1 here we are giving the total cuont in that map task*/
			for (String k : storageHash.keySet()) {
				IntWritable v = new IntWritable(storageHash.get(k));
				context.write(new Text(k), v);
			}
			// Part4 changes ends
		}
	}

	public static class IntSumReducer extends
			Reducer<Text, IntWritable, Text, IntWritable> {
		private IntWritable result = new IntWritable();

		public void reduce(Text key, Iterable<IntWritable> values,
				Context context) throws IOException, InterruptedException {
			int sum = 0;
			for (IntWritable val : values) {
				sum += val.get();
			}
			result.set(sum);
			context.write(key, result);
		}
	}

	// Aniket changes start
	/* This is a custom partitioner This is used to send the words to required partitioner
	   * So it sends all the words starting from m or M to partitioner 1
	   * Sends all the words starting from n or N to partitioner 2 and so on... 
	   * I am doing the % ReducerCount to see if there are less than 4 reducers available 
	   * and also to avoid divide by zero error.*/  
	public static class WordPartitioner extends Partitioner<Text, IntWritable> {

		@Override
		public int getPartition(Text key, IntWritable value, int ReducerCount) {
			String word = key.toString();
			char first = word.charAt(0);
			if (ReducerCount == 0)
				return 0;
			if (first == 'm' || first == 'M')
				return 0;
			if (first == 'n' || first == 'N')
				return 1 % ReducerCount;
			if (first == 'o' || first == 'O')
				return 2 % ReducerCount;
			if (first == 'p' || first == 'P')
				return 3 % ReducerCount;
			else
				return 4 % ReducerCount;

		}

	}

	// Aniket changes ends

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args)
				.getRemainingArgs();
		if (otherArgs.length != 2) {
			System.err.println("Usage: wordcount <in> <out>");
			System.exit(2);
		}
		Job job = new Job(conf, "word count");
		job.setJarByClass(PerMapTally.class);
		job.setMapperClass(TokenizerMapper.class);
		// Aniket changes starts
		/* Here the partitioner is being called*/
		job.setPartitionerClass(WordPartitioner.class);
		// Aniket changes ends
		// Part 4 Aniket changes starts
		/* Here I am just disabling the combiner */
		// job.setCombinerClass(IntSumReducer.class);
		// Part 4 Aniket changes ends
		job.setReducerClass(IntSumReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
