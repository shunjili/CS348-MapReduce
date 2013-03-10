import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.json.simple.JSONObject;
import org.json.simple.JSONValue;

/**
 * MapReduce job that pipes input to output as MapReduce-created key-val pairs
 * (c) 2012 Jeannie Albrecht
 * Modified by Laura Effinger-Dean, Winter 2013.
 */
public class ClickRate extends Configured implements Tool {
	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(), new ClickRate(), args);
		System.exit(res);
	}

	@Override
	public int run(String[] args) throws Exception {
		if (args.length < 2) {
			System.err.println("Error: Wrong number of parameters.");
			System.exit(1);
		}

		Configuration conf = this.getConf();
		Job job = new Job(conf, "trivial");

		job.setJarByClass(ClickRate.class);
		job.setMapperClass(ClickRate.AdMapper.class);
		job.setReducerClass(ClickRate.AdReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileInputFormat.addInputPath(job, new Path(args[1]));
		FileOutputFormat.setOutputPath(job, new Path("temp/"));

		job.waitForCompletion(true);
		Job secondJob = new Job(conf, "second");

		secondJob.setJarByClass(ClickRate.class);
		secondJob.setMapperClass(ClickRate.SecondMapper.class);
		secondJob.setReducerClass(ClickRate.SecondReducer.class);
		secondJob.setOutputKeyClass(Text.class);
		secondJob.setOutputValueClass(LongWritable.class);
		FileInputFormat.addInputPath(secondJob, new Path("temp/"));
		FileOutputFormat.setOutputPath(secondJob, new Path(args[2]));
		return secondJob.waitForCompletion(true)?  1:0;
	}

	/**
	 * map: (LongWritable, Text) --> (LongWritable, Text)
	 * 
	 * NOTE: Keys must implement WritableComparable, values must implement
	 * Writable
	 */
	public static class AdMapper extends
	Mapper<LongWritable, Text, Text, Text> {
		@Override
		public void map(LongWritable key, Text val, Context context)
				throws IOException, InterruptedException {
			//System.out.println("key=" + key + ", val=" + val);
			StringTokenizer tokens = new StringTokenizer(val.toString(), "\n");

			Text impressionKey = new Text();

			while(tokens.hasMoreElements()){
				Text output = new Text();
				JSONObject obj = (JSONObject) JSONValue.parse(tokens.nextToken().toString());
				String impressionId = obj.get("impressionId").toString();
				String adId = obj.get("adId").toString();
				Object referrer = obj.get("referrer");
				boolean isImpression = (referrer != null);
				String outputString; 
				if (isImpression){
					outputString = adId + " " + referrer.toString()+ " 0";
				}else{
					outputString = adId + " unknown" +" 1";
				}
				output.set(outputString);
				impressionKey.set(impressionId);
				context.write(impressionKey, output);
			}

		}
	}
	public static class SecondMapper extends
	Mapper<LongWritable, Text, Text, LongWritable> {
		@Override
		public void map(LongWritable key, Text val, Context context)
				throws IOException, InterruptedException {
			// key = [page_url, ad_id]
			StringTokenizer tokens = new StringTokenizer(val.toString(), "\n");

			while(tokens.hasMoreElements()){
				Text keyPair = new Text();
				String line = tokens.nextToken(); 
				String[] elementArray = line.split("\\s+"); 
				keyPair.set("["+elementArray[1]+ ", "+ elementArray[0]+ "]");
				LongWritable outVal = line.endsWith("1")? new LongWritable(1): new LongWritable(0); 
				// line format: impressionID, adId, referrer, 1/0 eg. "0QpkDJxbHsBIDMjUT33MdOiWAReilW	MK4Iugm2kx92sEwGV2SrN5T75mHl68 excite.co.jp 1"
				
				context.write(keyPair, outVal);
			}

		}
	}


	public static class AdReducer extends
	Reducer<Text, Text, Text, Text> {
		@Override
		public void reduce(Text key, Iterable<Text> value,
				Context context) throws IOException, InterruptedException {
			String output = "";
			for (Text val : value) {
				if(output == ""){
					output = val.toString();
				}else{
					output = MergeClicksImpression(output, val.toString());
				}
			}
			String[] strArray = output.split(" "); 
			Text newKey = new Text();
			newKey.set(strArray[0]);
			Text outputText = new Text(strArray[1]+ " " + strArray[2] );
			context.write(newKey, outputText);
		}
	}

	public static class SecondReducer extends
	Reducer<Text, LongWritable, Text, DoubleWritable> {
		@Override
		public void reduce(Text key, Iterable<LongWritable> value,
				Context context) throws IOException, InterruptedException {
			long total = 0;
			long count = 0; 
			for (LongWritable val : value) {
				if (val.get() == 1) {
					total += 1;
					count += 1;
				}else{
					total +=1; 
				}
			}
			double ratio = (double) count/total;
			context.write(key, new DoubleWritable(ratio));
		}
	}
	public static String  MergeClicksImpression(String output, String current){
		String[] array= output.split(" ");
		if(current.endsWith("1")) {
			array[2] = "1";
		}else{
			array[1] = current.split(" ")[1];
		}
		return array[0] + " " + array[1]+ " " + array[2];
	}
	
}