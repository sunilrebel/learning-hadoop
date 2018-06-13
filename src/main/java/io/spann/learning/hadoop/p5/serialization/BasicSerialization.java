package io.spann.learning.hadoop.p5.serialization;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.util.StringUtils;

import java.io.*;

public class BasicSerialization {

    public static void main(String[] args) throws IOException {
        Integer num = 163;
        IntWritable intWritable = new IntWritable(num);
        byte[] bytes = serialize(intWritable);
        System.out.println("Serialized Hex bytes of " + num + " is: " + StringUtils.byteToHexString(bytes));

        IntWritable intWritable1 = new IntWritable();
        deserialize(intWritable1, bytes);
        System.out.println("Deserialized number is: " + intWritable1);
    }

    public static byte[] serialize(Writable writable) throws IOException {
        ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
        DataOutputStream dataOutputStream = new DataOutputStream(byteArrayOutputStream);
        writable.write(dataOutputStream);
        dataOutputStream.close();
        return byteArrayOutputStream.toByteArray();
    }

    public static byte[] deserialize(Writable writable, byte[] bytes) throws IOException {
        ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(bytes);
        DataInputStream dataInputStream = new DataInputStream(byteArrayInputStream);
        writable.readFields(dataInputStream);
        dataInputStream.close();
        return bytes;
    }
}
