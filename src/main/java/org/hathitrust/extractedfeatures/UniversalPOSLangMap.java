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
	
	public abstract int size();
	
	public abstract boolean containsLanguage(String lang_key);

	public abstract String getUniversalLanguagePOSUnchecked(String lang_key, String opennlp_pos_key);
	
	public abstract String getUniversalLanguagePOSChecked(String lang_key, String opennlp_pos_key);

	public abstract Tuple2<String,String> getUniversalLanguagePOSPair(String[] lang_keys,String opennlp_pos_key);	

}
