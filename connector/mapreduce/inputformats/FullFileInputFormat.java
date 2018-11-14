package fr.edf.dco.common.connector.mapreduce.inputformats;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

/**
 * Small Text non splittable files input format definition
 * 
 * @author fahd-externe.essid@edf.fr
 */
public class FullFileInputFormat extends FileInputFormat<BytesWritable, BytesWritable> {

  @Override
  protected boolean isSplitable(JobContext context, Path file) {
    return false; 
  }

  @Override
  public RecordReader<BytesWritable, BytesWritable> createRecordReader (InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
    FullFileRecordReader reader = new FullFileRecordReader();
    reader.initialize(split, context);
    return reader;
  }
}
