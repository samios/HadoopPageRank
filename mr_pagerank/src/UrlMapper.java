
/**

Mapper : does much of the work for the analysis of WARC records read.

The following information iregarding WARC is copied from
from https://github.com/ept/warc-hadoop/blob/master/README.md
and the WARCInputFormat implementation used comes from Martin Kleppmann. 
Credits to him.


File format parsing
-------------------
A WARC file consists of a flat sequence of records. Each record may be a HTTP request (recordType = "request"), a response (recordType = "response") or one of various other types, including metadata. When reading from a WARC file, the records are given to the mapper one at a time. That means that the request and the response will appear in two separate calls of the map method.

This library currently doesn't perform any parsing of the data inside records, such as the HTTP headers or the HTML body. You can simply read the server's response as an array of bytes. Additional parsing functionality may be added in future versions.

WARC files are typically gzip-compressed. Gzip files are not splittable by Hadoop (i.e. an entire file must be processed sequentially, it's not possible to start reading in the middle of a file) so projects like CommonCrawl typically aim for a maximum file size of 1GB (compressed). If you're only doing basic parsing, a file of that size takes less than a minute to process.
a
When writing WARC files, this library automatically splits output files into gzipped segments of approximately 1GB. You can customize the segment size using the configuration key warc.output.segment.size (the value is the target segment size in bytes).

**/




import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;

import java.util.regex.Pattern;
import java.util.regex.Matcher;

import com.martinkl.warc.WARCWritable;
import com.martinkl.warc.mapreduce.WARCInputFormat;


public class UrlMapper extends Mapper<LongWritable, WARCWritable, Text, Text> {

	  private Pattern patternTag;
	  private Matcher matcherTag;
	  private StringTokenizer tokenizer;
	  private Text outKey = new Text();
	  private LongWritable outVal = new LongWritable(1);
	  private static final String HTML_URL_PATTERN = 
		"((href=')|(HREF=')|(HREF=\")?(href=\"))+(http://)?[a-zA-Z_0-9\\-]+(\\.\\w[a-zA-Z_0-9\\-]+)+(/[#&\\n\\-=?\\+\\%/\\.\\w]+)?";


	/**
 	* Extract hrefs found in html bodies and sum identical hrefs
 	**/
 	public void extractHrefs(LongWritable key, WARCWritable value, Context context) throws IOException,InterruptedException {

		patternTag = Pattern.compile(HTML_URL_PATTERN);
		String recordType = value.getRecord().getHeader().getRecordType();
        String targetURL  = value.getRecord().getHeader().getTargetURI();
 		if (recordType.equals("response")) {
			try {
				// Returns the body of the record, as an unparsed raw array of bytes.
				byte[] rawData = value.getRecord().getContent();
				String content = new String(rawData);
				// Only extract the body of the HTTP response when necessary
				// Due to the way strings work in Java, we don't use any more memory than before
				StringBuffer sb = new StringBuffer();
				String body = content.substring(content.indexOf("<body"),content.indexOf("</body>"));
				outKey.set(targetURL);

				// Process all the matched HTML tags found in the body of the document
				matcherTag = patternTag.matcher(body);
				while (matcherTag.find()) {
				    String url = matcherTag.group().replace("href=\"","").replace("href=\'","").replace("HREF=\'","").replace("HREF=\\","");
					sb.append(url+";");

				    
				}
				    context.write(outKey, new Text(sb.toString()));	

		}
		catch (Exception e) { }
		}
	}




   @Override
   /**
    * map - The entry point of the mapper.
    **/
    public void map(LongWritable key, WARCWritable value, Context context) {

	// -- extract URLs
	try {	
		extractHrefs(key, value, context);

	}
	catch (Exception e) {
		System.out.println("* Extraction of hrefs failed.");
		System.exit(1);
	}

    }
}

