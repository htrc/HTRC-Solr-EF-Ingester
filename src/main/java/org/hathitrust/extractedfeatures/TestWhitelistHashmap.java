package org.hathitrust.extractedfeatures;

import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.stream.Stream;


public class TestWhitelistHashmap {

	protected String _dictionary_filename;
	protected HashMap<String,Boolean> _hashmap;
	
	protected int FILL_FACTOR = 26;
	
	public TestWhitelistHashmap(String dictionary_filename) {
		System.out.println("Constructing: WhitelistHashmap");
	
		_dictionary_filename = dictionary_filename;
		
		_hashmap = new HashMap<String,Boolean>();
	}
	
	public void storeEntries() 
	{
		System.out.println("Build hashmap ...");
	
		for (int i=0; i<FILL_FACTOR; i++) {
			char prefix = (char) ('a' + i);
			
			//read file into stream, try-with-resources
			try (Stream<String> stream = Files.lines(Paths.get(_dictionary_filename))) {

				stream.forEach(word -> {_hashmap.put(prefix+word,true);});
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		System.out.println("... done");

	}
	
	public boolean contains(String key)
	{
		return _hashmap.containsKey(key);
	}

}
