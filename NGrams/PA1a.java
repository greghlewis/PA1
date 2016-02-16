// A list of unigrams sorted by unigrams in an alphabetical ascending order.
// Within the same unigram, the list should be sorted by YEAR in an ascending order.
// 
// Basically, input keys are <word, (Author, Year)>
// Basically, output keys are <word, Year>
// So, for context, it should be (word : year : author)
import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat; //this should get replaced by WholeFile types, yes?
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

//-------------------------------------------------------------------------------
public class PA1a {
	
//-------------------------------------------------------------------------------

	public static class myMapper extends Mapper<LongWritable, Text, Text, Text> {
	
		//where does WholeFileRecordReader come into play?
		
		Text value = new Text();
		Text word = new Text();
		
		@Override
		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			//get the Stuff
			String headerAndBody = new String(value.getBytes());
			
			//get Author
			int authorLine = headerAndBody.indexOf("Author");
			int authorEnd = headerAndBody.indexOf("/n", authorLine);
			int authorBegin = headerAndBody.indexOf(" ", authorLine+9);
			String author = headerAndBody.substring(authorBegin, authorEnd);
			
			//get Year
			int yearLine = headerAndBody.indexOf("Release Date:");
			int yearBreak = headerAndBody.indexOf(",", yearLine);
			String year = headerAndBody.substring(yearBreak+2, yearBreak+6);
					
			//get Book start
			int bookStart = headerAndBody.indexOf("***");
			int bookStart2 = headerAndBody.indexOf("***", bookStart+4);
			int bookStartReal = headerAndBody.indexOf("/n", bookStart2+4);
			
			//get Book end
			int bookEndReal = headerAndBody.indexOf("*** END");
			
			//define book string
			String book = headerAndBody.substring(bookStartReal, bookEndReal);
			
			//kill line-ends in book string
			//transform '.' to '_START_ _END_' in book string (only needed in bigrams)
			//remove special characters
			book.replace("\n", " ").replace("\r", "").replace(".", "_END_ _START_").replaceAll("[^a-zA-Z0-9]+", "");
			
			//bind words (key) to values
			// I believe this breaks down the massive string by spaces. Will need to do some digging.
			//StringTokenizer itr = new StringTokenizer(book);
			//while (itr.hasMoreTokens()){
			//	word.set(itr.nextToken());
				//writes to context (words, year)
			//	context.write(word, one);
			//}
			
			String[] tokenedBook = book.split("\\s");
			for (int x=0; x<tokenedBook.length; x++){
				word.set(tokenedBook[x]);
				value.set(year);
				context.write(word,value);
			}
			
		}
	}	

//---------------------------------------------------------------------------------

	public static class myReducer extends Reducer<Text, Text, Text, Text>{
	
		private Text result = new Text();
		
		@Override
		public void reduce(Text key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {
			//iterate through the list, binding more keys?
			
			//honestly no idea what's going on here, but it's what the java docs say, so...
			//I think this is the part where it adds up the word count to figure out which one comes first...
			String sum = null;
			for (Text val : values){
				sum += val.set();
			}
			result.set(sum);
			context.write(key, result);
		}
	}

//----------------------------------------------------------------------------------


	//this is where the Job gets done
	
	public static void main(String[] args) throws Exception{
		// TODO Auto-generated method stub
		
		// This is line-for-line copied from the "Word Count" example in the hadoop docs
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "word count"); //what does this do?
		job.setJarByClass(PA1a.class);
		job.setMapperClass(myMapper.class);
		job.setCombinerClass(myReducer.class);
		job.setReducerClass(myReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		FileInputFormat.addInputPath(job,  new Path(args[0]));
		FileOutputFormat.setOutputPath(job,  new Path(args[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
	
	

}