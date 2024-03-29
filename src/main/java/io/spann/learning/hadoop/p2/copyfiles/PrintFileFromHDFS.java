package io.spann.learning.hadoop.p2.copyfiles;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;

public class PrintFileFromHDFS {

    public static void main(String[] args) throws IOException {
        String uri = "hdfs://sandbox:8020/user/admin/work/ml-100k/u.item";
        Configuration conf = new Configuration();
//        conf.set("yarn.resourcemanager.address", "quickstart.cloudera:8050"); // see step 3
//        conf.set("mapreduce.framework.name", "yarn");
//        conf.set("fs.defaultFS", "quickstart.cloudera:8020");

        FileSystem fs = FileSystem.get(URI.create(uri), conf);

        InputStream in = fs.open(new Path(uri));
        IOUtils.copyBytes(in, System.out, 4096, false);
        IOUtils.closeStream(in);
    }
}
