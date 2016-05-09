import java.io.IOException;
import java.util.StringTokenizer;
import java.io.File;
import java.io.FileReader;
import java.io.BufferedReader;
import java.util.Vector;
import java.io.PrintWriter;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.reduce.LongSumReducer;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;
import org.apache.hadoop.mapred.lib.*;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import com.martinkl.warc.mapreduce.WARCInputFormat;
import com.martinkl.warc.WARCWritable;



public class PageRank {
    
    static class MultiFileOutput extends MultipleTextOutputFormat<Text, Text> {
            protected String generateFileNameForKeyValue(Text key, Text value,String name) {
                    return key.toString();
            }
    }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
    int NumUrl=0;
    if (otherArgs.length != 1) {
      System.err.println("Usage: UrlJob <in> <out>");
      System.err.println("       where <in> can be a set of files (e.g data/*.warc.gz");
      System.exit(2);
    }
/**   List of all urls	**/

    Job job1 = new Job(conf, "Url_Job1");
    job1.setJarByClass(PageRank.class);

    FileInputFormat.addInputPath(job1, new Path(otherArgs[0]));
    FileOutputFormat.setOutputPath(job1, new Path("outputjob1"));


    FileSystem fs = FileSystem.newInstance(conf);
    if (fs.exists(new Path("outputjob1"))) {
		fs.delete(new Path("outputjob1"), true);
    }

    job1.setInputFormatClass(WARCInputFormat.class);   // Our custom input split
    job1.setOutputFormatClass(TextOutputFormat.class); // The default


    job1.setMapOutputKeyClass(Text.class);  
    job1.setMapOutputValueClass(LongWritable.class);

    job1.setMapperClass(UrlListMapper.class);
    job1.setReducerClass(UrlListReducer.class);
	job1.waitForCompletion(true);

 
/*   Get links    */


    Job job2 = new Job(conf, "Url_Job2");
    job2.setJarByClass(PageRank.class);

    FileInputFormat.addInputPath(job2, new Path(otherArgs[0]));
    FileOutputFormat.setOutputPath(job2, new Path("outputjob2"));

	fs = FileSystem.newInstance(conf);
    if (fs.exists(new Path("outputjob2"))) {
		fs.delete(new Path("outputjob2"), true);
    }
    job2.setInputFormatClass(WARCInputFormat.class);   // Our custom input split
    job2.setOutputFormatClass(TextOutputFormat.class); // The default

    job2.setMapOutputKeyClass(Text.class);  
    job2.setMapOutputValueClass(Text.class);
	job2.setOutputKeyClass(Text.class);
	job2.setOutputValueClass(Text.class);

    job2.setMapperClass(UrlMapper.class);

    job2.setReducerClass(UrlReducer.class);
    job2.waitForCompletion(true);
  
/* Create adjency matrix */

    File file=new File("outputjob1/part-r-00000");
    Vector<String> node=new Vector<String>();
    int k=0;
    try (BufferedReader br = new BufferedReader(new FileReader(file))) {
        String line;
        while ((line = br.readLine()) != null) {
            node.add(line);
            
            k++;
        }
    }
    NumUrl=node.size();
    PrintWriter w = new PrintWriter("nodes", "UTF-8");
    for(int i=0;i<NumUrl;i++)
    {
       w.print(i+"\t"+node.get(i)+"\n");
         
    }


    file=new File("outputjob2/part-r-00000");
    int[][] am=new int[NumUrl][NumUrl];
    for(int i=0;i<NumUrl;i++)
    {
        for(int j=0;j<NumUrl;j++)
        {
            if(i==j)
            am[i][j]=1;
            else
            am[i][j]=0;
        }
    }

    try (BufferedReader br = new BufferedReader(new FileReader(file))) {
        String line;
        while ((line = br.readLine()) != null) {
        
        String page=line.split("\t")[0];
        String[] links=line.split("\t")[1].replace(" ","").split(";");
        for(int i=0;i<links.length;i++)
            am[node.indexOf(page)][node.indexOf(links[i])]=1;
        }
    }



        
/* Output matrix to file for next jobs */

PrintWriter writer = new PrintWriter("AM", "UTF-8");
    for(int i=0;i<NumUrl;i++)
    {
       writer.print(i+" "+1/(double)NumUrl+" ");
         
        for(int j=0;j<NumUrl;j++)
        {
            if(j<NumUrl-1)
            writer.print(am[i][j]+" ");
            else
            writer.println(am[i][j]);

        }
    }

writer.close();

/* Calculate the pagerank */
    int outputIndex = 0;
        
    long startTime = System.currentTimeMillis();
        
    conf.setInt("numUrls", NumUrl);
    int numReduceTasks = 1;

    for (int i=0;i<1;i++)
    {
        System.out.println("Hadoop PageRank iteration "+ i +"...\n");
        Job job3 = new Job(conf, "PageRank");
        //config.setInt("numUrls", numUrls);
        job3.setJarByClass(PageRank.class);
        job3.setMapperClass(PageRankMap.class);
        job3.setReducerClass(PageRankReduce.class);
        job3.setOutputKeyClass(LongWritable.class);
        job3.setOutputValueClass(Text.class);
        
        //the output in the current iteration will become input in next iteration.

        FileInputFormat.addInputPath(job3, new Path("AM"));
        FileOutputFormat.setOutputPath(job3, new Path("outputjob3"));


        fs = FileSystem.newInstance(conf);
        if (fs.exists(new Path("outputjob3"))) {
            fs.delete(new Path("outputjob3"), true);
        }        
            numReduceTasks = 1;
        job3.setNumReduceTasks(numReduceTasks);
        
        job3.waitForCompletion(true);
        if (!job3.isSuccessful()){
            System.out.format("Hadoop PageRank iteration:{"+ i +"} failed, exit...", i);
            System.exit(-1);
        }
        //clean the intermediate data directories.
        fs.delete(new Path("outputjob1"), true);
        fs.delete(new Path("outputjob2"), true);

        outputIndex++;
        }

  }
}
