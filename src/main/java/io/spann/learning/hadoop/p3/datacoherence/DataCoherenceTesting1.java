package io.spann.learning.hadoop.p3.datacoherence;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.net.URI;

public class DataCoherenceTesting1 {
    public static void main(String[] args) throws IOException {
        String uri = "hdfs://quickstart.cloudera/sunil";
        FileSystem fs = FileSystem.get(URI.create(uri), new Configuration());

        Path p = new Path("hdfs://quickstart.cloudera/sunil/newfile");
        fs.create(p);
        assert (fs.exists(p)) : "Failed assertion";
    }
}
