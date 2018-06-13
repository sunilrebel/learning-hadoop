package io.spann.learning.hadoop.p3.datacoherence;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.io.OutputStream;
import java.net.URI;

public class DataCoherenceTesting2 {
    public static void main(String[] args) throws IOException {
        String uri = "hdfs://quickstart.cloudera/sunil";
        FileSystem fs = FileSystem.get(URI.create(uri), new Configuration());

        Path p = new Path("hdfs://quickstart.cloudera/sunil/newfile");
        FSDataOutputStream out = fs.create(p);
        out.write("content".getBytes("UTF-8"));
        out.flush();
        out.sync();

        assert (fs.getFileStatus(p).getLen() == 0L) : "Failed assertion";
    }
}
