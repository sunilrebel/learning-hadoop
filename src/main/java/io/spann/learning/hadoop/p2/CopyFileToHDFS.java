package io.spann.learning.hadoop.p2;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.util.Progressable;

import java.io.*;
import java.net.URI;

public class CopyFileToHDFS {
    public static void main(String[] args) throws IOException {
        String sourceFilePath = "/video/ep01/IMG_7365.MOV";
        String destinationFolder = "hdfs://quickstart.cloudera:8020/sunil/hdfs/copyviaprogram/mymovie.mov";

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
