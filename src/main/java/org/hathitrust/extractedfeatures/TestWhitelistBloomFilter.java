package org.hathitrust.extractedfeatures;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.stream.Stream;

import com.google.common.hash.BloomFilter;
import com.google.common.hash.Funnel;
import com.google.common.hash.Funnels;

public class TestWhitelistBloomFilter {
	
	protected String _dictionary_filename;
	protected BloomFilter<CharSequence> _bloomFilter;
	
	protected int FILL_FACTOR = 26;
	
	// http://stackoverflow.com/questions/453018/number-of-lines-in-a-file-in-java
		public static int countLines(String filename) throws IOException 
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
		
		
	public TestWhitelistBloomFilter(String dictionary_filename) {
		System.out.println("Constructing: WhitelistBloomFilter");
	
		_dictionary_filename = dictionary_filename;
		
		System.out.println("Counting lines in: " + dictionary_filename);
		int num_lines;
		try {
			num_lines = countLines(dictionary_filename);
			
			Funnel<CharSequence> string_funnel = Funnels.stringFunnel(StandardCharsets.UTF_8);
			_bloomFilter = BloomFilter.create(string_funnel, FILL_FACTOR * num_lines,0.01);
			
		} catch (IOException e) {
			
			e.printStackTrace();
		}
	
	}
	
	public void storeEntries() 
	{
		System.out.println("Building synthetically expanded Bloom filter ...");
		
		for (int i=0; i<FILL_FACTOR; i++) {
			
			char prefix = (char) ('a' + i);
			
			//read file into stream, try-with-resources
			try (Stream<String> stream = Files.lines(Paths.get(_dictionary_filename))) {

				stream.forEach(word -> {_bloomFilter.put(prefix+word);});
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		System.out.println("... done");

	}
	
	public boolean contains(String key)
	{
		return _bloomFilter.mightContain(key);
	}
	
	public void serializeOut(String filename)
	{
		try {
			FileOutputStream fos = new FileOutputStream(filename);

			BufferedOutputStream bfos = new BufferedOutputStream(fos);

			_bloomFilter.writeTo(bfos);
			
			bfos.close();
		}
		catch (FileNotFoundException e) {
			System.err.println("Unable to open Bloom file:" + filename);
			e.printStackTrace();
		} catch (IOException e) {
			System.err.println("Error reading in Bloom file:" + filename);
			e.printStackTrace();
		}
	}
	
	public void serializeIn(String filename)
	{
		try {
			FileInputStream fis = new FileInputStream(filename);

			BufferedInputStream bfis = new BufferedInputStream(fis);

			Funnel<CharSequence> string_funnel = Funnels.stringFunnel(StandardCharsets.UTF_8);
			_bloomFilter = BloomFilter.readFrom(bfis,string_funnel);
			
			bfis.close();
		}
		catch (FileNotFoundException e) {
			System.err.println("Unable to open Bloom file:" + filename);
			e.printStackTrace();
		} catch (IOException e) {
			System.err.println("Error writing out Bloom file:" + filename);
			e.printStackTrace();
		}
	}
	
	
}