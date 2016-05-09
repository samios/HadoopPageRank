

import java.io.BufferedWriter;
import java.io.IOException;
import java.util.*;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
 

public class PageRankMap extends Mapper<LongWritable, Text, LongWritable, Text> {

//each map task handles one line within an adjacency matrix file
//key: file offset
//value: <sourceUrl PageRank#targetUrls>
	public void map(LongWritable key, Text value, Context context)
		throws IOException, InterruptedException {

			int numUrls = context.getConfiguration().getInt("numUrls",1);

			String line = value.toString();

			// instance an object that records the information for one webpage

			RankRecord rrd = new RankRecord(line);

			int sourceUrl, targetUrl;
			// double rankValueOfSrcUrl;
			StringBuilder mapOutput = new StringBuilder();
			
			if (rrd.targetUrlsList.size()<=0){
				// there is no out degree for this webpage; 
				// scatter its rank value to all other urls
				double rankValuePerUrl = rrd.rankValue/(double)numUrls;
				for (int i=0;i<numUrls;i++){
					context.write(new LongWritable(i), new Text(String.valueOf(rankValuePerUrl)));
				}
			} else {
				double rankValuePerTargetUrl = rrd.rankValue/(double)rrd.targetUrlsList.size();
				Iterator<Integer> targetUrlsIterator = rrd.targetUrlsList.iterator();
				while (targetUrlsIterator.hasNext()) {
					long nextUrl = targetUrlsIterator.next();
					context.write(new LongWritable(nextUrl), new Text(String.valueOf(rankValuePerTargetUrl))); //output <targetUrl, rankValuePerTargetURL value> pair
					mapOutput.append("#" + nextUrl);
				}
				context.write(new LongWritable(rrd.sourceUrl), new Text(mapOutput.toString())); //output <sourceUrl, #targetURLs> pair
			} //for
		} // end map

}
