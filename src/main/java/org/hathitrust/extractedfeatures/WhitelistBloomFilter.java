package org.hathitrust.extractedfeatures;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.stream.Stream;

import com.google.common.hash.BloomFilter;
import com.google.common.hash.Funnel;
import com.google.common.hash.Funnels;

public class WhitelistBloomFilter {

	protected BloomFilter<CharSequence> _bloomFilter;
	protected static final String SERIALIZED_SUFFIX = "-serialized";
	protected static final double FALSE_POSITIVE_PERCENTAGE = 0.01;
	
	// http://stackoverflow.com/questions/453018/number-of-lines-in-a-file-in-java
	protected static int countLines(String filename) throws IOException 
	{
		InputStream is = new BufferedInputStream(new FileInputStream(filename));

		try {
			byte[] c = new byte[1024];
			int count = 0;
			int readChars = 0;
			boolean empty = true;
			while ((readChars = is.read(c)) != -1) {
				empty = false;
				for (int i = 0; i < readChars; ++i) {
					if (c[i] == '\n') {
						++count;
					}
				}
			}
			return (count == 0 && !empty) ? 1 : count;
		} finally {
			is.close();
		}
	}


	public WhitelistBloomFilter(String dictionary_filename, boolean serialize) {
		System.out.println("Constructing: WhitelistBloomFilter");
		
		String ser_dictionary_filename = dictionary_filename + SERIALIZED_SUFFIX;
		
		if (ClusterFileIO.exists(ser_dictionary_filename)) {
			System.out.println("Loading Serialized Bloom filter ...");
			_bloomFilter = serializeIn(ser_dictionary_filename);
			System.out.println("... done");
		}
		else {
			// Need to generate the Bloom filter from the given raw text file
			
			System.out.println("Counting lines in: " + dictionary_filename);
			int num_lines = -1;
			try {
				num_lines = countLines(dictionary_filename);
				 
				Funnel<CharSequence> string_funnel = Funnels.stringFunnel(StandardCharsets.UTF_8);
				_bloomFilter = BloomFilter.create(string_funnel, num_lines,FALSE_POSITIVE_PERCENTAGE);
			} 
			catch (IOException e) {
				e.printStackTrace();
			}
			System.out.println("Number of lines: " + num_lines);
			
			storeEntries(dictionary_filename,serialize);
		}

	}

	protected void storeEntries(String filename, boolean serialize) 
	{
		System.out.println("Building Bloom filter ...");

		//read file into stream, try-with-resources
		try (Stream<String> stream = Files.lines(Paths.get(filename))) {
			stream.forEach(word -> {_bloomFilter.put(word);});
		} catch (IOException e) {
			e.printStackTrace();
		}

		System.out.println("... done");
		
		if (serialize) {
			System.out.println("Serializing Bloom filter ...");

			String ser_filename = filename + SERIALIZED_SUFFIX;
			serializeOut(ser_filename);

			System.out.println("... done");
		}

	}

	public boolean contains(String key)
	{
		return _bloomFilter.mightContain(key);
	}

	protected void serializeOut(String ser_filename)
	{
		try {
		    BufferedOutputStream bos = ClusterFileIO.getBufferedOutputStream(ser_filename);
			_bloomFilter.writeTo(bos);
			bos.close();
		}
		catch (FileNotFoundException e) {
			System.err.println("Unable to open Bloom file:" + ser_filename);
			//e.printStackTrace();
		} catch (IOException e) {
			System.err.println("Error reading in Bloom file:" + ser_filename);
			//e.printStackTrace();
		}
	}

	protected static BloomFilter<CharSequence> serializeIn(String ser_filename)
	{
		BloomFilter<CharSequence> bloomFilter = null;
	
		try {
		    BufferedInputStream bis = ClusterFileIO.getBufferedInputStream(ser_filename);
		  
			Funnel<CharSequence> string_funnel = Funnels.stringFunnel(StandardCharsets.UTF_8);
			bloomFilter = BloomFilter.readFrom(bis,string_funnel);

			bis.close();
		}
		catch (FileNotFoundException e) {
			System.err.println("Unable to open Bloom file:" + ser_filename);	
			//e.printStackTrace();
		} catch (IOException e) {
			System.err.println("Error writing out Bloom file:" + ser_filename);
			//e.printStackTrace();
		}
		return bloomFilter;
	}
}