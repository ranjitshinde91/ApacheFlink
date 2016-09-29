package com.demo.flink.library;
import java.io.IOException;

import org.xerial.snappy.Snappy;


public class SnappyJava {

	public static void main(String[] args) throws IOException {
		String input = "Hello snappy-java! Snappy-java is a JNI-based wrapper of "
			     		+ "Snappy, a fast compresser/decompresser.";
			byte[] compressed = Snappy.compress(input.getBytes("UTF-8"));
			byte[] uncompressed =  Snappy.uncompress(compressed);

			String result = new String(uncompressed, "UTF-8");
			System.out.println(result);
	}
}

