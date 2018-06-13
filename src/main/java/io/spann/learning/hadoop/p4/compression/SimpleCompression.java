package io.spann.learning.hadoop.p4.compression;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionOutputStream;
import org.apache.hadoop.util.ReflectionUtils;

import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URI;
import java.util.logging.Logger;

/**
 * Execution command:
 * rm -rf /tmp/file && echo "Test" | hadoop jar /tmp/LearningHadoop-1.0.0-jar-with-dependencies.jar io.spann.learning.hadoop.p4.compression.SimpleCompression org.apache.hadoop.io.compress.GzipCodec /tmp/file.gz && gunzip /tmp/file.gz && cat /tmp/file
 * <p/>
 * rm -rf /tmp/file && echo "Test" | hadoop jar /tmp/LearningHadoop-1.0.0-jar-with-dependencies.jar io.spann.learning.hadoop.p4.compression.SimpleCompression org.apache.hadoop.io.compress.BZip2Codec /tmp/file.bz2 && bzip2 -d /tmp/file.bz2 && cat /tmp/file
 * <p/>
 * rm -rf /tmp/file.lz4 && echo "Test" | hadoop jar /tmp/LearningHadoop-1.0.0-jar-with-dependencies.jar io.spann.learning.hadoop.p4.compression.SimpleCompression org.apache.hadoop.io.compress.Lz4Codec /tmp/file.lz4 && hadoop fs -text file:///tmp/file.lz4
 * <p/>
 * rm -rf /tmp/file.snappy && echo "Test" | hadoop jar /tmp/LearningHadoop-1.0.0-jar-with-dependencies.jar io.spann.learning.hadoop.p4.compression.SimpleCompression org.apache.hadoop.io.compress.SnappyCodec /tmp/file.snappy && hadoop fs -text file:///tmp/file.snappy
 */
public class SimpleCompression {

    public static void main(String[] args) throws ClassNotFoundException, IOException {
        String compressionClass = args[0];
        String output = args[1];

        Logger log = Logger.getLogger(SimpleCompression.class.getName());
        log.info("Using compression: " + compressionClass);

        Class<?> codeClass = Class.forName(compressionClass);
        Configuration conf = new Configuration();
        CompressionCodec codec = (CompressionCodec) ReflectionUtils.newInstance(codeClass, conf);

        String outputFilePath = URI.create(output).getPath();
        FileOutputStream outputStream = new FileOutputStream(outputFilePath);

        CompressionOutputStream out = codec.createOutputStream(outputStream);
        IOUtils.copyBytes(System.in, out, 4096, false);
        out.finish();
    }
}
