package org.hathitrust.extractedfeatures;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;

import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.util.DoubleAccumulator;
import org.json.JSONArray;
import org.json.JSONObject;

class PerVolumeCatalogLangStreamFlatmap implements FlatMapFunction<String, String>
{
	private static final long serialVersionUID = 1L;
	
	protected String _input_dir;
	protected int    _verbosity;
	
	protected DoubleAccumulator _progress_accum;
	protected double            _progress_step;
	
	boolean _strict_file_io;
	
	public PerVolumeCatalogLangStreamFlatmap(String input_dir, int verbosity, 
					 		  DoubleAccumulator progress_accum, double progress_step,
					 		  boolean strict_file_io)
	{
		_input_dir  = input_dir;
		_verbosity  = verbosity;
		
		_progress_accum = progress_accum;
		_progress_step  = progress_step;
		
		_strict_file_io = strict_file_io;
	}
	
	public Iterator<String> call(String json_file_in) throws IOException
	{ 
	     
		String full_json_file_in = _input_dir + "/" + json_file_in;
		JSONObject extracted_feature_record = JSONClusterFileIO.readJSONFile(full_json_file_in);
		
		ArrayList<String> catalog_lang_list = new ArrayList<String>();
		
		if (extracted_feature_record != null) {
			String volume_id = extracted_feature_record.getString("id");

			JSONObject ef_metadata = extracted_feature_record.getJSONObject("metadata");

			if (_verbosity >= 1) {
				System.out.println("Processing: " + json_file_in);
			}

			if (ef_metadata != null) {
				String ef_catalog_language = ef_metadata.getString("language");
				if (!ef_catalog_language.equals("")) {

					catalog_lang_list.add(ef_catalog_language);
				}
				else {
					System.err.println("No catalog 'language' metadata => Skipping id: " + volume_id);
				}
			}
			else {
				System.err.println("No 'metadata' section in JSON file => Skipping id: " + volume_id);
			}
			
		}
		else {
			// File did not exist, or could not be parsed
			String mess = "Failed to read in bzipped JSON file '" + full_json_file_in + "'";
			if (_strict_file_io) {
				throw new IOException(mess);
			}
			else {
				System.err.println("Warning: " + mess);
				System.out.println("Warning: " + mess);
			}
		}

		_progress_accum.add(_progress_step);
		
		return catalog_lang_list.iterator();
	}
	
	
}

