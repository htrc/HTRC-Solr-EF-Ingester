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

public abstract class UniversalPOSLangMap 
{
	protected HashMap<String,Integer> _missing_pos;
	
	public UniversalPOSLangMap() 
	{	
		_missing_pos = new HashMap<String,Integer>();
	}
	
	protected List<Path> readMapFiles(String langmap_directory)
	{
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
	                .filter(filePath->filePath.getFileName().endsWith(".map"))
	                //.filter(filePath->!filePath.getFileName().endsWith("~"))
	                .collect(Collectors.toList());
			
			System.out.println("Filtered language maps to read in: " + langmap_paths);

			
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		return langmap_paths;
	}
	
	protected void fileMapToPOSLookup(Path langmap_path, HashMap<String,String> pos_lookup )
	{		
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
	}
	
	public abstract int size();
	
	public abstract boolean containsLanguage(String lang_key);

	public abstract String getUniversalLanguagePOSUnchecked(String lang_key, String opennlp_pos_key);
	
	public abstract String getUniversalLanguagePOSChecked(String lang_key, String opennlp_pos_key);

	public abstract Tuple2<String,String> getUniversalLanguagePOSPair(String[] lang_keys,String opennlp_pos_key);	

}
