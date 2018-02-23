package org.hathitrust.extractedfeatures;

import java.io.BufferedReader;

import org.json.JSONObject;

public class JSONClusterFileIO extends ClusterFileIO {

	protected static JSONObject readJSONFile(String filename)
	{
		JSONObject json_obj = null;
			
		try {
			StringBuilder sb = new StringBuilder();
			
			BufferedReader br = ClusterFileIO.getBufferedReaderForCompressedFile(filename);

			int cp;
			while ((cp = br.read()) != -1) {
			    sb.append((char) cp);
			}

			/*
			String str;
			while ((str = br.readLine()) != null) {
				sb.append(str);
			}
			*/
	
	        br.close();
	        
	        json_obj = new JSONObject(sb.toString());
		} 
		catch (Exception e) {
			e.printStackTrace();
		}
		
		return json_obj;
	}

}
