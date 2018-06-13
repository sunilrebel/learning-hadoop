package io.spann.learning.hadoop.p4.compression;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.io.compress.CompressionInputStream;

import java.io.*;
import java.net.URI;
import java.util.logging.Logger;

/**
 * Usage:
 * hadoop jar /tmp/LearningHadoop-1.0.0-jar-with-dependencies.jar io.spann.learning.hadoop.p4.compression.CodecFactoryExample file:///tmp/file.snappy
 */
public class CodecFactoryExample {

    public static void main(String[] args) throws IOException {
        String inputPath = args[0];

        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(URI.create(inputPath), conf);
        CompressionCodecFactory compressionCodecFactory = new CompressionCodecFactory(conf);
        Path filePath = new Path(inputPath);
        CompressionCodec codec = compressionCodecFactory.getCodec(filePath);

        if (codec == null) {
            Logger.getLogger(CodecFactoryExample.class.getName()).severe("Not able to identify correct coded for file");
            System.exit(1);
        }

        String outputPath = CompressionCodecFactory.removeSuffix(inputPath, codec.getDefaultExtension());

        CompressionInputStream in = codec.createInputStream(fs.open(filePath));
        FSDataOutputStream out = fs.create(new Path(outputPath));

        IOUtils.copyBytes(in, out, conf, true);
    }
}
