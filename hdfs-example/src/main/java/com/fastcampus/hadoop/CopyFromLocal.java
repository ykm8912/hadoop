package com.fastcampus.hadoop;

import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

public class CopyFromLocal {
    public static void main(String[] args) throws Exception{
        String localSrc = args[0];
        String dst = args[1];
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(URI.create(dst), conf);
        // fs.copyFromLocalFile(new Path(localSrc), new Path(dst));

        InputStream in = new BufferedInputStream(new FileInputStream(localSrc));
        OutputStream out = fs.create(new Path(dst));

        IOUtils.copyBytes(in, out, 4096, true);
    }
}
