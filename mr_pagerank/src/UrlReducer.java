
import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.log4j.Logger;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;





public class UrlReducer extends Reducer<Text, LongWritable, Text, Text> {


    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException,InterruptedException {
	
			Text outputValue = values.iterator().next();

	context.write(key, outputValue);	

    }
}

