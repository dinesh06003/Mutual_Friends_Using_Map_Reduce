import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map.Entry;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
public class SecondOne{

	public static class Map 
			extends Mapper<LongWritable, Text, Text, Text>{
		Text user = new Text();
		Text friends = new Text();
		private int LinestoRead;
		private int linesProcessed = 0;
		public static int maxfrnds = 0;
		
		@Override
	    protected void setup(Context context) throws IOException, InterruptedException {
	        super.setup(context);
	        Configuration conf = context.getConfiguration();
	        LinestoRead = Integer.parseInt(conf.get("numoflines")); // Read 'n' from configuration
	    }
		
		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException{
			if(linesProcessed < LinestoRead) {
			String[] split=value.toString().split("\\t");
			String userId=split[0];
			int len = split.length;
			if(len==1) {
				return;
			}
			String[] friendIds=split[1].split(",");
			for(String friend : friendIds) {
				if(userId.equals(friend)) {
					continue;
				}
			String userKey = (Integer.parseInt(userId) < Integer.parseInt(friend))?userId + "," +friend : friend + ","+ userId;
			String regexExpression="((\\b"+ friend + "[^\\w]+)|\\b,?" + friend + "$)";
			friends.set(split[1].replaceAll(regexExpression, ""));
			user.set(userKey);
			context.write(user,friends);
			}
			linesProcessed++;
			}
		}
	}
		
	public static class Reduce
			extends Reducer<Text, Text, Text, Text>{
		
		int mutualFriendSize = 0;
		int maxMutualFriendSize = 0;
		HashMap<String,String> hash=new HashMap<>();
		
		private String matchingFriends(String firstList, String secondList) {
			
			if(firstList == null || secondList == null) {
				return null;
			}
			
			String[] list1=firstList.split(",");
			String[] list2=secondList.split(",");
			
			LinkedHashSet<String> firstSet = new LinkedHashSet<String>();
			for(String  user: list1) {
				firstSet.add(user);
			}
			LinkedHashSet<String> secondSet = new LinkedHashSet<String>();
			for(String  user: list2) {
				secondSet.add(user);
			}
			firstSet.retainAll(secondSet);
			mutualFriendSize = firstSet.size();
			return firstSet.toString().replaceAll("\\[|\\]", "");
		}
		
		public void reduce(Text key, Iterable<Text> values, Context context)
			throws IOException,InterruptedException{
			
			String[] friendsList = new String[2];
			int index=0;
			
			for(Text value:values) {
				friendsList[index++] = value.toString();
			}
			
			String mutualFriends = matchingFriends(friendsList[0],friendsList[1]);
			
			hash.put(key.toString(), mutualFriendSize + "");
			if(maxMutualFriendSize < mutualFriendSize)
				maxMutualFriendSize = mutualFriendSize;
		}
		
		public void cleanup(Context context) throws IOException, InterruptedException {
			List<Entry<String, String>> countList = new ArrayList<Entry<String, String>>(hash.entrySet());
			
			for(Entry<String, String> entry: countList) {
				if(maxMutualFriendSize == Integer.parseInt(entry.getValue()))
					context.write(new Text(entry.getKey()), new Text(entry.getValue()));
		    }	
		}
	
	}

	
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		int linestoread = Integer.parseInt(args[2]);
		conf.set("numoflines", String.valueOf(linestoread));
		Job job = new Job(conf, "soc-LiveJournal1");
		job.setJarByClass(MutualFriends.class);
		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		long startTime = System.currentTimeMillis();
	    if (!job.waitForCompletion(true)) {
	        System.exit(1);
	    }
	    long endTime = System.currentTimeMillis();
	    long executionTime = endTime - startTime;
	    System.out.println("Runtime in ms for Part21:"+executionTime);	}
}