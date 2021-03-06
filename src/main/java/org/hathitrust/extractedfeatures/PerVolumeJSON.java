package org.hathitrust.extractedfeatures;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;

import org.apache.hadoop.io.Text;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.util.DoubleAccumulator;
import org.json.JSONArray;
import org.json.JSONObject;

/*
class PagedJSON implements Function<String, Boolean> {

	private static final long serialVersionUID = 1L;

	public Boolean call(String s) { return s.contains("a"); }
}
 */


//public class PerVolumeJSON implements VoidFunction<String> 
public class PerVolumeJSON implements Function<Text,Integer>
{
	private static final long serialVersionUID = 1L;
	protected PerVolumeUtil _per_vol_util;

	public PerVolumeJSON(String input_dir, String whitelist_filename, String langmap_directory,
				         ArrayList<String> solr_endpoints, String output_dir, int verbosity, 
					     boolean icu_tokenize, boolean strict_file_io)
	{
		
		// Had issues with class not found in Spark when set up with inheritance
		_per_vol_util = new PerVolumeUtil(input_dir, whitelist_filename, langmap_directory,
				  solr_endpoints, output_dir, verbosity,
				  icu_tokenize, strict_file_io);
	
	}
	
	
	public Integer call(Text json_text) throws IOException

	{ 
		return _per_vol_util.call(json_text);
	}
}

