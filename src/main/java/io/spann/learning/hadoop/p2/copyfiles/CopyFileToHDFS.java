package io.spann.learning.hadoop.p2.copyfiles;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.util.Progressable;

import java.io.*;
import java.net.URI;

public class CopyFileToHDFS {
    public static void main(String[] args) throws IOException {
        String sourceFilePath = "/Users/deepti/sunil/learning/traininglearning/ml-100k/u.user";
        String destinationFolder = "hdfs://sandbox-hdp.hortonworks.com:8020/user/admin/work/ml-100k/u.user";

        InputStream in = new BufferedInputStream(new FileInputStream(sourceFilePath));

        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(URI.create(destinationFolder), conf);
        OutputStream out = fs.create(new Path(destinationFolder), new Progressable() {
            public void progress() {
                System.out.print(".");
            }
        });

        IOUtils.copyBytes(in, out, 4096, true);
    }
}
