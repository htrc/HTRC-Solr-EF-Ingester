package org.hathitrust.extractedfeatures;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import scala.Tuple2;

public class UniversalPOSLangMapHeterogeneous extends UniversalPOSLangMap
{

	protected HashMap<String,HashMap<String,String>> _all_langmaps;
	
	
	public UniversalPOSLangMapHeterogeneous(String langmap_directory)
	{
		super();
		
		System.out.println("Constructing: UniversalPOS Language Map for Heterogeneous");
				
		_all_langmaps = new HashMap<String,HashMap<String,String>>();
		 
		List<Path> langmap_paths = null;
		
		URI langmap_directory_uri = null;
		
		try {
			langmap_directory_uri = new URI(langmap_directory);
		}
		catch (Exception e) {
			e.printStackTrace();
		}
			
		Path langmap_directory_path = null;
		try {
			// Spark/Hadoop friendly
			langmap_directory_path = Paths.get(langmap_directory_uri);
		}
		catch (Exception e) {
			// Relative local file-system friendly
			langmap_directory_path = Paths.get(langmap_directory_uri.getRawPath());
		}
		
		
		try (Stream<Path> stream_paths = Files.walk(langmap_directory_path)) {
			langmap_paths = stream_paths
	                .filter(Files::isRegularFile)
	                .collect(Collectors.toList());

		} catch (IOException e) {
			e.printStackTrace();
		}
		
		// For-each language file
		langmap_paths.forEach(langmap_path -> {
			File langmap_file = langmap_path.toFile();
			String lang_key = langmap_file.getName().substring(0,2);
			
			HashMap<String,String> pos_lookup = new HashMap<String,String>();
			
			// For-each line within that language file
			try (Stream<String> lang_lines = Files.lines(langmap_path)) {
				lang_lines.forEach(line -> {
					if ((line.length()>0) && (!line.matches("^\\s+$")) && !line.startsWith("#")) {
						String[] line_parts = line.split("\\t");
						if (line_parts.length == 2) {
							String pos_key = line_parts[0];
							String pos_val = line_parts[1];
							pos_lookup.put(pos_key, pos_val);
						}
					}
				});
			} catch (IOException e) {
				e.printStackTrace();
			}

			_all_langmaps.put(lang_key, pos_lookup);
		});

		System.out.println("Done Constructing UniversalPOS Language Map for heterogeneous POS set");
		
	}
	
	public int size()
	{
		return _all_langmaps.size();
	}
	
	public boolean containsLanguage(String lang_key)
	{
		return _all_langmaps.containsKey(lang_key);
	}

	public String getUniversalLanguagePOSUnchecked(String lang_key,String heterogeneous_pos_key)
	{
		String universal_pos = null;
		
		HashMap<String,String> langmap = _all_langmaps.get(lang_key);
		if (langmap != null) {
			universal_pos = langmap.get(heterogeneous_pos_key);
		}
		
		return universal_pos;
	} 
	
	public String getUniversalLanguagePOSChecked(String lang_key,String heterogeneous_pos_key)
	{
		if (!_all_langmaps.containsKey(lang_key)) {
			// Not a language with a POS map
			return "";
		}
		
		String universal_pos = null;
		
		HashMap<String,String> langmap = _all_langmaps.get(lang_key);
		universal_pos = langmap.get(heterogeneous_pos_key);
		
		if (universal_pos == null) {
			String missing_lang_pos = lang_key + ":" + heterogeneous_pos_key;

			// Maintain some stats on how often the POS for this language is missing
			Integer mpos_freq = 0;
			if (_missing_pos.containsKey(missing_lang_pos)) {
				mpos_freq = _missing_pos.get(missing_lang_pos);
			}
			else {
				System.err.println("Warning: for language key '"+lang_key
						+"' failed to find POS '" + heterogeneous_pos_key + "'");
				System.err.println("Defaulting to POS 'X' (i.e., 'other')");
			}
			mpos_freq++;
			_missing_pos.put(missing_lang_pos,mpos_freq);

			universal_pos = "X";
		}
		
		return universal_pos;
	} 
	
	public Tuple2<String,String> getUniversalLanguagePOSPair(String[] lang_keys,String heterogeneous_pos_key)
	{
		String universal_pos = null;
		String selected_lang = null;
		
		for (int li=0; li<lang_keys.length; li++) {
			String lang_key = lang_keys[li];
			
			universal_pos = getUniversalLanguagePOSUnchecked(lang_key,heterogeneous_pos_key);
			if (universal_pos != null) {
				selected_lang = lang_key;
				break;
			}
		}
	
		if (universal_pos == null) {
			// Failed to any match in any of the given languages
			// => Lock onto the first language (highest probability when modeled)
			selected_lang = lang_keys[0];
			
			if (!_all_langmaps.containsKey(selected_lang)) {
				// Not a language with a POS map
				return new Tuple2<String,String>(selected_lang,null);
			}
			
			// If here, then is a POS language => default to "X" 
			
			String missing_lang_pos = selected_lang + ":" + heterogeneous_pos_key;

			// Maintain some stats on how often the POS for this language is missing
			Integer mpos_freq = 0;
			if (_missing_pos.containsKey(missing_lang_pos)) {
				mpos_freq = _missing_pos.get(missing_lang_pos);
			}
			else {
				System.err.println("Warning: for language key '"+selected_lang
						+"' failed to find POS '" + heterogeneous_pos_key + "'");
				System.err.println("Defaulting to POS 'X' (i.e., 'other')");
			}
			mpos_freq++;
			_missing_pos.put(missing_lang_pos,mpos_freq);

			universal_pos = "X";
		}

		return new Tuple2<String,String>(selected_lang,universal_pos);
	} 
}
