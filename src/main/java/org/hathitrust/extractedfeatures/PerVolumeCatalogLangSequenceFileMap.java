package org.hathitrust.extractedfeatures;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.spark.api.java.function.Function;
import org.json.JSONObject;

class PerVolumeCatalogLangSequenceFileMap implements Function<Text, String>
{
	private static final long serialVersionUID = 1L;
	
	protected String _input_dir;
	protected int    _verbosity;
	
	boolean _strict_file_io;
	
	public PerVolumeCatalogLangSequenceFileMap(String input_dir, int verbosity, boolean strict_file_io)
	{
		_input_dir  = input_dir;
		_verbosity  = verbosity;
		
		_strict_file_io = strict_file_io;
	}
	
	public String call(Text json_text) throws IOException
	{
		String catalog_lang = null;
		 
		try {
			JSONObject extracted_feature_record  = new JSONObject(json_text.toString());

			String volume_id = extracted_feature_record.getString("id");

			JSONObject ef_metadata = extracted_feature_record.getJSONObject("metadata");

			if (_verbosity >= 1) {
				System.out.println("Processing: " + volume_id);
			}

			if (ef_metadata != null) {
				String ef_catalog_language = ef_metadata.getString("language");
				if (!ef_catalog_language.equals("")) {

					catalog_lang = ef_catalog_language;
				}
				else {
					System.err.println("No catalog 'language' metadata => Skipping id: " + volume_id);
				}
			}
			else {
				System.err.println("No 'metadata' section in JSON file => Skipping id: " + volume_id);
			}

		}
		catch (Exception e) {
			if (_strict_file_io) {
				throw e;
			}
			else {
				e.printStackTrace();
			}
		}
		
		return catalog_lang;
	}
}

