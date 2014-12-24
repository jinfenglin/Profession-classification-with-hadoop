package code;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.filecache.DistributedCache;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
public class TestMaper {
	public static class TestMapper extends Mapper<LongWritable, Text, Text, Text> {	
		public static Set<String> people_set=new HashSet<String>();
		protected void setup(Mapper<LongWritable,Text, Text, Text>.Context context) throws IOException, InterruptedException 
		{
			//java.net.URI[] localPath =DistributedCache.getCacheFiles(context.getConfiguration());
			Path[] localPath =DistributedCache.getLocalCacheFiles(context.getConfiguration());
			File f_peopeltest =new File(localPath[0].toString());
			BufferedReader br1 = new BufferedReader(new InputStreamReader(new FileInputStream(f_peopeltest)));
			String line= new String("");
			//make_map(context);
			while((line=br1.readLine())!= null)
			{
				if(line=="")
					continue;
				people_set.add(line);
			}
			br1.close();
			super.setup(context);
		}

		@Override
		public void map(LongWritable articleId, Text indices, Context context) throws IOException,InterruptedException 
		{
			String line=indices.toString();
			if(line=="")
				return;
			int name_index=line.indexOf('<');
			if(name_index<0)
				return;
			String name=line.substring(0,name_index).trim();
			if(people_set.contains(name))
			{
				int index=line.indexOf('<');
				if(index<0)
					return;
				line=line.substring(index);
				context.write(new Text(name),new Text(line));
			}
		}
		
	}
	public static Map<String,Integer> Make_article_map(final String str)
	{
		int start_index=str.indexOf('<');
		Map<String,Integer> result=new HashMap<String,Integer>();
		while(start_index>0)
		{
			int next_index=str.indexOf('<', start_index+1);
			int word_index=str.indexOf(',', start_index);
			String word=str.substring(start_index+1, word_index);
			int time_index=str.indexOf('>', start_index);
			int time =Integer.parseInt(str.substring(word_index+1, time_index));
			result.put(word, time);
			if(next_index<0)
				break;
			start_index=next_index;
		}
		return result;
	}
	public static String Get_name_from_lemma(final String  str)
	{
		String name="";
		int name_index=str.indexOf(',');
		if(name_index<0)
			throw new RuntimeException("Can't find index for name in Get_name_from_lemma!");
		name=str.substring(0,name_index);
		return name;
	}
	public static float Get_py_from_lemma(final String str)
	{
		float py=0;
		int start_index,end_index;
		start_index=str.indexOf(',');
		end_index=str.indexOf('<');
		if(start_index>=end_index-1)
			throw new RuntimeException("Wrong index for name in Get_py_from_lemma!");
		py=Float.parseFloat(str.substring(start_index+1,end_index));
		return py;
		
	}
	
	public static class TestReducer extends Reducer<Text, Text, Text, Text> {
		static Map<String,Map <String,Integer>> profession_map=new HashMap<String,Map <String,Integer>>();
		static Map<String,Float> py_map=new HashMap<String,Float>();
		public void setup(Context context) throws IOException, InterruptedException
		{
			make_map(context);
			super.setup(context);
			
		}
		public static void make_map(Context context) throws IOException
		{
			Path pt= new Path("/user/hadoop11/assignment2/output/part-r-00000");
			//Path pt= new Path("/home/jinfenglin/Downloads/assigment2output/part-r-00000");
			FileSystem fs =FileSystem.get(context.getConfiguration());
			String line="";
			BufferedReader br2;
			try {
				br2 = new BufferedReader(new InputStreamReader(fs.open(pt)));
				int i=0;
					while((line=br2.readLine())!= null)
					{
						if(line=="")
							continue;
						System.out.println("p:"+i);
						i++;
						String name=Get_name_from_lemma(line);
						float py=Get_py_from_lemma(line);
						py_map.put(name,py);
						Map <String,Integer> word_freq_map=new HashMap<String,Integer>();
						word_freq_map=Make_article_map(line);
						profession_map.put(name, word_freq_map);
						//load name into set
						}
					System.out.println("Profession index done!");
					br2.close();
					if(profession_map.size()==0)
						throw new RuntimeException("Empty profession_map in make_map!");
			} 
			catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		@Override
		public void reduce(Text article_name, Iterable<Text> word_freq, Context context)
				throws IOException, InterruptedException {
			String result="";
			//make_map(context);
			for (Text it : word_freq) //for every word in the name <>,<>   it run for only once
			{
				String line=it.toString();
				Map <String,Float> score=new HashMap<String,Float>();
				Map <String,Integer> voca=new HashMap<String,Integer>();
				int total=0;
				if(profession_map.size()==0)
					throw new RuntimeException("Empty profession_map!");
				
				for(String set_it:profession_map.keySet()) //init the score map and get the vocabulary of each profession
				{
					int count=0;
					for(String iter:profession_map.get(set_it).keySet())
					{
						count+=profession_map.get(set_it).get(iter);	
					}
					total+=count;
					voca.put(set_it, count);
					score.put(set_it,py_map.get(set_it));
				}
				int length=line.length();
				while(!line.isEmpty())
				{
					
					int word_index=line.indexOf(',');
					int number_index=line.indexOf('>');
					String word=line.substring(1,word_index);
					String number=line.substring(word_index+1,number_index);
					line=line.replaceFirst('<'+word+','+number+'>', "");
					line=line.replaceFirst(",", "");
					
					if(length==line.length())
						break;
					else
						length=line.length();
					
					for(String set_it:profession_map.keySet())//updata the score of all class
					{
						float delt=0;
						int time=Integer.parseInt(number);
						if(profession_map.get(set_it).containsKey(word))//word is in the p(y|x)
						{
							int numerator=profession_map.get(set_it).get(word);
							int nominator=voca.get(set_it);
							float percent=numerator/(float)nominator;
							delt=(float) Math.log(percent);
						}
						else
							delt=(float) Math.log(1/(float)total);//smooth function
						score.put(set_it, score.get(set_it)+delt*time);
					}
				}
				//sort the map and output the result
				Map<String,Float> treemap=new TreeMap<String,Float>(score);
				int n=0;
				for (Iterator i = sortByValue(treemap).iterator(); i.hasNext(); ) {
		            String key =i.next().toString().trim();
		            n++;
		            if(n>3)
		            	break;
		            if(n==1)
		            	result=result+':'+key;
		            else
		            	result=result+','+key;
		        }
			}
			context.write(article_name, new Text(result));
		}
	}
	 public static List sortByValue(final Map m) {
	        List keys = new ArrayList();
	        keys.addAll(m.keySet());
	        Collections.sort(keys, new Comparator() {
	            public int compare(Object o1, Object o2) {
	                Object v1 = m.get(o1);
	                Object v2 = m.get(o2);
	                if (v1 == null) {
	                    return (v2 == null) ? 0 : 1;
	                }
	                else if (v1 instanceof Comparable) {
	                    return ((Comparable) v2).compareTo(v1);
	                }
	                else {
	                    return 0;
	                }
	            }
	        });
	        return keys;
	    }
	
    public static void main(String[] args) {
		// TODO: you should implement the Job Configuration and Job call
		// here
		Job job;
		try {
			job = new Job(new Configuration());		
			job.setJobName("Assignment2_testing");
			job.setJarByClass(TestMaper.class);
			job.setMapperClass(TestMapper.class);
			job.setReducerClass(TestReducer.class);
			
			job.setMapOutputKeyClass(Text.class);
			job.setMapOutputValueClass(Text.class);
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(Text.class);
			
			job.setInputFormatClass(TextInputFormat.class);
			job.setOutputFormatClass(TextOutputFormat.class);
			
			DistributedCache.addCacheFile(new Path("/user/hadoop11/assignment2/profession_test.txt").toUri(), job.getConfiguration());//cache article_index
			//DistributedCache.addCacheFile(new Path("/home/jinfenglin/Downloads/profession_test.txt").toUri(), job.getConfiguration());
			FileInputFormat.setInputPaths(job, new Path(args[0]));
			FileOutputFormat.setOutputPath(job, new Path(args[1]));
			job.getConfiguration().set("mapreduce.job.queuename","hadoop11");
			job.waitForCompletion(true);
			} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}
		

