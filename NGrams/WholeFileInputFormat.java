import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;

public class WholeFileInputFormat extends FileInputFormat <NullWritable, BytesWritable>{

protected boolean isSplitable(JobContext context, Path file){
    return false;
}

public RecordReader<NullWritable, BytesWritable> createRecordReader(
        InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
    WholeFileRecordReader reader = new WholeFileRecordReader();
    reader.initialize(split, context);
    return reader;
}

@Override
public RecordReader<NullWritable, BytesWritable> getRecordReader(InputSplit arg0, JobConf arg1, Reporter arg2)
		throws IOException {
	// TODO Auto-generated method stub
	return null;
}
}