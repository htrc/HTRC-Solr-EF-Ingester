package org.hathitrust.extractedfeatures;

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

// *** !!!
// Written, but not extensively tested, as processing EF 2.0 turned out to
// need the Heterogeneous version of mapping afterall
// *** !!!

public class UniversalPOSLangMapHomogeneous extends UniversalPOSLangMap
{

	protected HashMap<String,String> _pos_lookup;
	
	
	public UniversalPOSLangMapHomogeneous(String langmap_directory) 
	{
		super();
		
		System.out.println("Constructing: UniversalPOS Language Map for homogeneous POS set");
		
		_pos_lookup = new HashMap<String,String>();
		 
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

			// For-each line within that language file
			try (Stream<String> lang_lines = Files.lines(langmap_path)) {
				lang_lines.forEach(line -> {
					if ((line.length()>0) && (!line.matches("^\\s+$")) && !line.startsWith("#")) {
						String[] line_parts = line.split("\\t");
						if (line_parts.length == 2) {
							String pos_key = line_parts[0];
							String pos_val = line_parts[1];
							_pos_lookup.put(pos_key, pos_val);
						}
					}
				});
			} catch (IOException e) {
				e.printStackTrace();
			}
		});

		System.out.println("Done Constructing UniversalPOS Language Map for homogeneous POS map");
		
	}
	
	public int size()
	{
		return 1;
	}
	
	public boolean containsLanguage(String lang_key)
	{
		// Always returning 'true' here is OK, as if EF2 hasn't been able to determine a language
		// for a page of text, it sets all the POS labels for that page to "UKN"
		// => we can spot "UKN" in the get-lookup methods below, and respond accordingly
		return true; 
	}

	public String getUniversalLanguagePOSUnchecked(String lang_key, String homogeneous_pos_key)
	{
		// In unchecked version, OK to return 'null' if POS-key not present
		String universal_pos = _pos_lookup.get(homogeneous_pos_key); 
		
		return universal_pos;
	} 
	
	public String getUniversalLanguagePOSChecked(String lang_key, String homogeneous_pos_key)
	{
		if (homogeneous_pos_key.equals("UKN")) {
			// Not a language for which POS tagging was possible 
			return "";
		}
		
		String universal_pos = null;
		
		//HashMap<String,String> langmap = _all_langmaps.get(lang_key);
		universal_pos = _pos_lookup.get(homogeneous_pos_key);
		
		if (universal_pos == null) {
			String missing_lang_pos = lang_key + ":" + homogeneous_pos_key;

			// Maintain some stats on how often the POS for this language is missing
			Integer mpos_freq = 0;
			if (_missing_pos.containsKey(missing_lang_pos)) {
				mpos_freq = _missing_pos.get(missing_lang_pos);
			}
			else {
				System.err.println("Warning: for language key '"+lang_key
						+"' failed to find POS '" + homogeneous_pos_key + "'");
				System.err.println("Defaulting to POS 'X' (i.e., 'other')");
			}
			mpos_freq++;
			_missing_pos.put(missing_lang_pos,mpos_freq);

			universal_pos = "X";
		}
		
		return universal_pos;
	} 
	
	
	public Tuple2<String,String> getUniversalLanguagePOSPair(String[] lang_keys,String homogeneous_pos_key)
	{
		String universal_pos = null;
		String selected_lang = null;
		
		for (int li=0; li<lang_keys.length; li++) {
			String lang_key = lang_keys[li];
			
			universal_pos = getUniversalLanguagePOSUnchecked(lang_key,homogeneous_pos_key);
			if (universal_pos != null) {
				selected_lang = lang_key;
				break;
			}
		}
	
		if (universal_pos == null) {
			// Failed to any match in any of the given languages
			// => Lock onto the first language (highest probability when modeled)
			selected_lang = lang_keys[0];
			
			if (!homogeneous_pos_key.equals("UKN")) {
				// Not a language where POS tagging has occurred
				return new Tuple2<String,String>(selected_lang,null);
			}
			
			// If here, then is a POS language => default to "X" 
			
			String missing_lang_pos = selected_lang + ":" + homogeneous_pos_key;

			// Maintain some stats on how often the POS for this language is missing
			Integer mpos_freq = 0;
			if (_missing_pos.containsKey(missing_lang_pos)) {
				mpos_freq = _missing_pos.get(missing_lang_pos);
			}
			else {
				System.err.println("Warning: for language key '"+selected_lang
						+"' failed to find POS '" + homogeneous_pos_key + "'");
				System.err.println("Defaulting to POS 'X' (i.e., 'other')");
			}
			mpos_freq++;
			_missing_pos.put(missing_lang_pos,mpos_freq);

			universal_pos = "X";
		}

		return new Tuple2<String,String>(selected_lang,universal_pos);
	} 
	
	
}
