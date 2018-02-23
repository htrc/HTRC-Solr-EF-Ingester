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

public class PerVolumeJSONList implements Function<String,Integer>
{
	private static final long serialVersionUID = 1L;
	protected PerVolumeUtil _per_vol_util;

	public PerVolumeJSONList(String input_dir, String whitelist_filename, String langmap_directory,
				         ArrayList<String> solr_endpoints, String output_dir, int verbosity, 
					     boolean icu_tokenize, boolean strict_file_io)
	{
		_per_vol_util = new PerVolumeUtil(input_dir, whitelist_filename, langmap_directory,
										  solr_endpoints, output_dir, verbosity,
										  icu_tokenize, strict_file_io);
				
	}
	
	public Integer call(String json_file_in) throws IOException
	{
		// Read in JSON file as Text
		String full_json_file_in = _per_vol_util.getInputDir() + "/" + json_file_in;
		String json_content_string = ClusterFileIO.readTextFile(full_json_file_in);
		
		Text json_content_text = new Text(json_content_string);
	
		return _per_vol_util.call(json_content_text);
	}
}

