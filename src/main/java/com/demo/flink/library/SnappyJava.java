package com.demo.flink.library;
import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xerial.snappy.Snappy;

public class SnappyJava {
	private static Logger LOGGER = LoggerFactory.getLogger(SnappyJava.class);

	public static void main(String[] args) throws IOException {
		String input = "Hello snappy-java! Snappy-java is a JNI-based wrapper of "
			     		+ "Snappy, a fast compresser/decompresser.";
		byte[] compressed = Snappy.compress(input.getBytes("UTF-8"));
		byte[] uncompressed =  Snappy.uncompress(compressed);
		
		LOGGER.info("compressed length :" +compressed.length);
		LOGGER.info("uncompressed length :"+ uncompressed.length);

		String result = new String(uncompressed, "UTF-8");
		LOGGER.info(result);
	}
}

