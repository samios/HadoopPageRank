
import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.log4j.Logger;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.NullWritable;





public class UrlListReducer extends Reducer<Text, LongWritable, Text, LongWritable> {


   @Override
    public void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException,InterruptedException {
									
	context.write(key, null);	

    }
}

