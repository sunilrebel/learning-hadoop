package io.spann.learning.hadoop.p6.writablecollections;

import org.apache.hadoop.io.*;

import java.io.IOException;

public class SimpleCollections {

    public static void main(String[] args) throws IOException {
        MapWritable src = new MapWritable();
        src.put(new IntWritable(1), new Text("Cat"));
        src.put(new VIntWritable(2), new LongWritable(163));

        GenericWritable genericWritable = new GenericWritable() {
            @Override
            protected Class<? extends Writable>[] getTypes() {
                return new Class[]{IntWritable.class, Text.class, LongWritable.class};
            }
        };
        genericWritable.set(new IntWritable(5));

        ArrayWritable genericValueList = new ArrayWritable(GenericWritable.class);
        genericValueList.set(new Writable[]{genericWritable});
//        src.put(new IntWritable(3), genericValueList);


        MapWritable dest = new MapWritable();
        WritableUtils.cloneInto(dest, src);

        System.out.print(dest.get(new IntWritable(1)));
        System.out.print(dest.get(new VIntWritable(2)));
        System.out.print(dest.get(new IntWritable(3)));
    }
}
