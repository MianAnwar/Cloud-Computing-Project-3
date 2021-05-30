import java.io.BufferedReader;
import java.io.IOException;
import java.io.FileWriter;
import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.util.LinkedHashMap;
import java.util.Map;
import org.json.simple.JSONArray;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.HashMap;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;

public class TwittersTweetsSentiment_Analysis extends Configured implements Tool {
	
	/*
		Mapper Class for Twitter Tweets
	*/ 
	public static class Map extends Mapper<LongWritable, Text, Text, Text>{
		private URI[] files;
		
		// Datastore
		private HashMap<String,Tweets> AFINN_map = new HashMap<String,Tweets>();
		private ArrayList<String> positiveWords = new ArrayList<String>();
		private ArrayList<String> negativeWords = new ArrayList<String>();
		private ArrayList<String> stopWords = new ArrayList<String>();
		
		// setup and load the Tweets, positive, negative & stop in memory
		@Override
		public void setup(Context context) throws IOException
		{
			files = DistributedCache.getCacheFiles(context.getConfiguration());
			Path path = new Path(files[0]);
			FileSystem fs = FileSystem.get(context.getConfiguration());
			FSDataInputStream in = fs.open(path);
			BufferedReader br = new BufferedReader(new InputStreamReader(in));

			String line="";
			while((line = br.readLine())!=null)
			{
				// Method 1: parsing into a JSON element
				JsonObject jsonObject = new JsonParser().parse(json).getAsJsonObject();
				System.out.println(jsonObject.get("name").getAsString());

				// Method 2: parsing into a Java Object
				Tweets Tweets = new Gson().fromJson(json, Tweets.class);

				AFINN_map.put(tweets.id, Tweets);
			}
			br.close();
			in.close();

			loadPositiveWords(context, 'positive_words.txt');
			loadNegativeWords(context, 'positive_words.txt');
			loadStopWords(context, 'stop_words.txt');
		}
		
		public void loadPositiveWords(Context context, String filepath) throws IOException
		{
			Path path = new Path(filepath);
			FileSystem fs = FileSystem.get(context.getConfiguration());
			FSDataInputStream in = fs.open(path);
			BufferedReader br = new BufferedReader(new InputStreamReader(in));

			String line="";
			while((line = br.readLine())!=null)
			{
				positiveWords.add(tweets);
			}
			br.close();
			in.close();
		}

		public void loadNegativeWords(Context context, String filepath) throws IOException
		{
			Path path = new Path(filepath);
			FileSystem fs = FileSystem.get(context.getConfiguration());
			FSDataInputStream in = fs.open(path);
			BufferedReader br = new BufferedReader(new InputStreamReader(in));

			String line="";
			while((line = br.readLine())!=null)
			{
				negativeWords.add(tweets);
			}
			br.close();
			in.close();
		}

		public void loadStopWords(Context context, String filepath) throws IOException
		{
			Path path = new Path(filepath);
			FileSystem fs = FileSystem.get(context.getConfiguration());
			FSDataInputStream in = fs.open(path);
			BufferedReader br = new BufferedReader(new InputStreamReader(in));

			String line="";
			while((line = br.readLine())!=null)
			{
				stopWords.add(tweets);
			}
			br.close();
			in.close();
		}


		// mapper 
		public void map(LongWritable key, HashMap<String,Tweets> value, Context context) throws IOException, InterruptedException{
			Tweets tweets = new Tweets();

			int sentiment_sum = 0;
			int positive = 0;
			int negative = 0;
			try{
				for(int i=0;i<value.length /* length of words */; i++){

					JSONObject obj =(JSONObject) jsonParser.parse(line);
					String tweet_id = (String) obj.get("get_id");
					String tweet_text=(String) obj.get("social_feed");
					for(String word:positiveWords ){
						if(tweet_text.contain(word))
						{
							positive++;
						}
					}
					for(String word:negativeWords ){
						if(tweet_text.contain(word))
						{
							negative++;
						}
					}
					// formula
					sentiment_sum = (positive - negative) / (positive + negative +1);
					value.sentiment_score = sentiment_sum;
				}
			}catch(Exception e){
				e.printStackTrace();
			}
		}
	}


 
	/*
		Reducer Class
	*/ 
	public static class Reduce extends Reducer<Text,Text,Text,Text>{
		public void reduce(Text key, HashMap<String,Tweets> value, Context context) throws IOException, InterruptedException{
			//
			// A tweet i will be positive if si is greater than 0,  negative if si is less than 0 and neutral if si is equal to 0
			//
			try{
				String ResultpositiveJson;
				for(int i=0; i<value.length; i++)
				{
					ResultpositiveJson +=value.split('\n')[0];
				}
				    
				context.write(key, ResultpositiveJson);
				
			}catch(Exception e)
			{
				// System.out.println(e);
			}
			
		}
	}



	/* 
		Driver
	*/
	public static void main(String[] args) throws Exception{
		ToolRunner.run(new TwittersTweetsSentiment_Analysis(),args);
	}


	@Override
	public int run(String[] args) throws Exception {
	// TODO Auto-generated method stub
		Configuration conf = new Configuration();
		if (args.length != 2) {
		System.exit(2);
		}
		DistributedCache.addCacheFile(new URI("/twitter.json"),conf);
		Job job = new Job(conf, "SentimentAnalysis");
		job.setJarByClass(TwittersTweetsSentiment_Analysis .class);
		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(Text.class);
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
		return 0;
	}

}