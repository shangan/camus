package com.linkedin.camus.etl.kafka.common;

import com.linkedin.camus.coders.CamusWrapper;
import com.linkedin.camus.etl.IEtlKey;
import com.linkedin.camus.etl.RecordWriterProvider;
import com.linkedin.camus.etl.kafka.mapred.EtlMultiOutputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.ReflectionUtils;

import java.io.DataOutputStream;
import java.io.IOException;


/**
 * Provides a RecordWriter that uses FSDataOutputStream to write
 * a String record as bytes to HDFS without any reformatting or compession.
 */
public class StringRecordWriterProvider implements RecordWriterProvider {
    public static final String ETL_OUTPUT_RECORD_DELIMITER = "etl.output.record.delimiter";
    public static final String DEFAULT_RECORD_DELIMITER    = "";

    protected String recordDelimiter = null;
    private String fileNameExtension = "";

    // TODO: Make this configurable somehow.
    // To do this, we'd have to make RecordWriterProvider have an
    // init(JobContext context) method signature that EtlMultiOutputFormat would always call.
    @Override
    public String getFilenameExtension(TaskAttemptContext context) {
      Configuration conf = context.getConfiguration();
      boolean isCompressed = FileOutputFormat.getCompressOutput(context);
      CompressionCodec codec = null;
      if (isCompressed) {
        Class<? extends CompressionCodec> codecClass =
                FileOutputFormat.getOutputCompressorClass(context, GzipCodec.class);
        codec = (CompressionCodec) ReflectionUtils.newInstance(codecClass, conf);
        fileNameExtension = codec.getDefaultExtension();
      }
      return fileNameExtension;
    }

    @Override
    public RecordWriter<IEtlKey, CamusWrapper> getDataRecordWriter(
            TaskAttemptContext  context,
            String              fileName,
            CamusWrapper        camusWrapper,
            FileOutputCommitter committer) throws IOException, InterruptedException {

        // If recordDelimiter hasn't been initialized, do so now
        if (recordDelimiter == null) {
            recordDelimiter = context.getConfiguration().get(
                ETL_OUTPUT_RECORD_DELIMITER,
                DEFAULT_RECORD_DELIMITER
            );
        }

        Configuration conf = context.getConfiguration();
        boolean isCompressed = FileOutputFormat.getCompressOutput(context);
        CompressionCodec codec = null;
        if (isCompressed) {
          Class<? extends CompressionCodec> codecClass =
                  FileOutputFormat.getOutputCompressorClass(context, GzipCodec.class);
          codec = (CompressionCodec) ReflectionUtils.newInstance(codecClass, conf);
          fileNameExtension = codec.getDefaultExtension();
        }


          // Get the filename for this RecordWriter.
          Path file = new Path(
              committer.getWorkPath(),
              EtlMultiOutputFormat.getUniqueFile(
                  context, fileName, fileNameExtension
              )
          );


        FileSystem fs = file.getFileSystem(conf);
        final DataOutputStream writer;
        if (!isCompressed) {
          writer = fs.create(file, false);
        } else {
          FSDataOutputStream fileOut = fs.create(file, false);
          writer = new DataOutputStream(codec.createOutputStream(fileOut));
        }


        // Return a new anonymous RecordWriter that uses the
        // FSDataOutputStream writer to write bytes straight into path.
        return new RecordWriter<IEtlKey, CamusWrapper>() {

            @Override
            public void write(IEtlKey ignore, CamusWrapper data) throws IOException {
                String record = (String)data.getRecord() + recordDelimiter;
                writer.write(record.getBytes());
            }

            @Override
            public void close(TaskAttemptContext context) throws IOException, InterruptedException {
                writer.close();
            }
        };
    }
}
