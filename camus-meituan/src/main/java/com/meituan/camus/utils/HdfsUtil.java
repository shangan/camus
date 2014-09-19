package com.meituan.camus.utils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.util.Progressable;
import org.apache.log4j.Logger;

import java.io.*;
import java.net.URI;

/**
 * Created by chenshangan on 14-6-18.
 */
public class HdfsUtil {

  private Configuration conf;
  private final static Logger log = Logger.getLogger(HdfsUtil.class);

  public HdfsUtil() {
    this.conf = new Configuration(true);
  }

  public HdfsUtil(Configuration conf) {
    this.conf = conf;
  }

  public byte[] readFromHdfs(String filePath) throws IOException {
    FileSystem fs = FileSystem.get(URI.create(filePath), conf);
    FSDataInputStream in = fs.open(new Path(filePath));
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    byte[] buf = new byte[1024];
    int len = 0;
    while ((len = in.read(buf)) > 0) {
      baos.write(buf, 0, len);
    }
    byte[] ret = baos.toByteArray();
    in.close();
    baos.close();
    fs.close();
    return ret;
  }

  public void copyToHdfs(String localFilePath, String destFilePath) throws IOException {

    InputStream in = new BufferedInputStream(new FileInputStream(localFilePath));

    FileSystem fs = FileSystem.get(URI.create(destFilePath), conf);
    OutputStream out = fs.create(new Path(destFilePath), new Progressable() {
      public void progress() {
        System.out.print(".");
      }
    });
    IOUtils.copyBytes(in, out, 4096, true);
    fs.close();
  }


  public void appendToHdfs(String destFilePath, byte[] data) throws IOException {
    FileSystem fs = FileSystem.get(URI.create(destFilePath), conf);
    FSDataOutputStream out = fs.append(new Path(destFilePath));
    out.write(data, 0, data.length);
    out.close();
    fs.close();
  }

  public void deleteFromHdfs(String filePath) throws IOException {
    FileSystem fs = FileSystem.get(URI.create(filePath), conf);
    fs.deleteOnExit(new Path(filePath));
    fs.close();
  }

  public FileStatus[] getDirectoryFromHdfs(String dir) throws IOException {
    FileSystem fs = FileSystem.get(URI.create(dir), conf);
    FileStatus[] fileStatuses = fs.listStatus(new Path(dir));
    return fileStatuses;
  }

  public FileSystem getFileSystem() throws IOException {
    return FileSystem.get(conf);
  }


}
