package org.hathitrust.extractedfeatures;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.stream.Stream;

import org.apache.commons.cli.*;
import org.apache.hadoop.io.Text;

public class ProcessForSerialSolrIngest 
{
	//private static final long serialVersionUID = 1L;
	
	protected String _input_file;
	protected String _solr_base_url;
	protected String _solr_collection;
	
	protected String _whitelist_filename;
	protected String _langmap_directory;
	
	//protected String _solr_url;
	protected String _output_dir;
	
	protected int    _verbosity;

	public ProcessForSerialSolrIngest(String input_file, String solr_collection,
									  String solr_base_url, String output_dir, int verbosity)
	{
		_input_file = input_file;
		_solr_collection = solr_collection;
		
		boolean use_whitelist = Boolean.getBoolean("wcsa-ef-ingest.use-whitelist");
		_whitelist_filename = (use_whitelist) ?  System.getProperty("wcsa-ef-ingest.whitelist-filename") : null;
		
		boolean use_langmap = Boolean.getBoolean("wcsa-ef-ingest.use-langmap");
		_langmap_directory = (use_langmap) ?  System.getProperty("wcsa-ef-ingest.langmap-directory") : null;
		
		
		_solr_base_url   = solr_base_url;
		_output_dir = output_dir;
		_verbosity  = verbosity;
	}

	protected String generateAppName()
	{
		String app_name = "Extract Features: Process for Serial Solr Ingest";
		app_name += " [" + _solr_collection + "]";

		if (_solr_base_url != null) { 
			app_name += " solr_base_url=" + _solr_base_url;
		}
		
		if (_output_dir != null) { 
			app_name += " output_dir=" + _output_dir;
		}
		
		return app_name;
	}
	
	public ArrayList<String> extrapolateSolrEndpoints(String solr_collection)
	{
		ArrayList<String> solr_endpoints = new ArrayList<String>();
		
		if (_solr_base_url != null) {
			String solr_url = _solr_base_url + "/" + solr_collection + "/update";
			
			String solr_cloud_nodes = System.getProperty("wcsa-ef-ingest.solr-cloud-nodes",null);
			if (solr_cloud_nodes != null) {
				String [] cloud_nodes = solr_cloud_nodes.split(",");
				for (String cn : cloud_nodes) {
					String solr_endpoint = solr_url.replaceFirst("//.*?:\\d+/", "//"+cn+"/");
					solr_endpoints.add(solr_endpoint);
				}
			}
			else {
				solr_endpoints.add(solr_url);
			}
		}
		
		return solr_endpoints;
	}

	public ArrayList<String> readFileLines(Path list_path)
	{
		ArrayList<String> json_file_list = new ArrayList<String>();
		
		try (Stream<String> list_lines = Files.lines(list_path)) {
			list_lines.forEach(line -> {
				json_file_list.add(line);
			});
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		return json_file_list;
		
	}
	
	public Text readJSONText(Path json_path)
	{
		File json_file = json_path.toFile();
	
		String json_filename = json_file.toURI().toString();
		/*
		try {
			json_filename = json_file.getCanonicalPath();
		}
		catch (Exception e) {
			e.printStackTrace();
		}
		*/
		String text_string = ClusterFileIO.readTextFile(json_filename);
		
		//ArrayList<String> text_lines = readFileLines(json_path);
		
		//String text_string = String.join("\n",text_lines);
		
		Text json_text = new Text(text_string);
		return json_text;
		
	}
	
	public void execPerVolumeSequenceFile()
	{
		String serial_app_name = generateAppName();		
		System.out.println(serial_app_name);
		
		Path json_filelist_path = Paths.get(_input_file);
		
		// Read in text file
		ArrayList<String> json_file_list = readFileLines(json_filelist_path);
		
		boolean icu_tokenize = Boolean.getBoolean("wcsa-ef-ingest.icu-tokenize");
		boolean strict_file_io = Boolean.getBoolean("wcsa-ef-ingest.strict-file-io");
		
		ArrayList<String> solr_endpoints = extrapolateSolrEndpoints(_solr_collection);
		
		System.out.println("*** away to create PerVolumeJSON class, _langmap_directory = " + _langmap_directory);
		PerVolumeJSON per_vol_json = new PerVolumeJSON(_input_file,_whitelist_filename, _langmap_directory,
											           solr_endpoints,_output_dir,_verbosity,
											           icu_tokenize,strict_file_io);
		
		// Foreach file, call per_vol_json.call()
		long num_vol_ids = 0;
		long json_file_list_len = json_file_list.size();
		for (String json_filename : json_file_list) {
			//Path json_path = Paths.get("file://D:/cygwin64/home/davidb/research/code-managed/hathitrust/wcsa/extracted-features-solr/trunk/solr-ingest/json-files",json_filename);
			Path json_path = Paths.get("json-files",json_filename);
			
			
			System.out.println("Processing jsonfile: " + json_path);
			Text json_text = readJSONText(json_path);
			try {
				per_vol_json.call(json_text);
			} catch (IOException e) {
				e.printStackTrace();
			}
			num_vol_ids++;
			System.out.println("+ Processed " + num_vol_ids + "/" + json_file_list_len);
		}
	
		
		System.out.println("");
		System.out.println("############");
		System.out.println("# Number of volume ids: " + num_vol_ids);
		System.out.println("############");
		System.out.println("");

	
		
		
	}
		

	public static void print_usage(HelpFormatter formatter, Options options)
	{
		formatter.printHelp("RUN.bash [options] input-file solr-collection", options);
	}
	
	public static void main(String[] args) {
		Options options = new Options();

		Option verbosity_opt = new Option("v", "verbosity", true, 
				"Set to control the level of debugging output [0=none, 1=some, 2=lots]");
		verbosity_opt.setRequired(false);
		options.addOption(verbosity_opt);
		
		Option properties_opt = new Option("p", "properties", true, 
				"Read in the specified Java properties file");
		properties_opt.setRequired(false);
		options.addOption(properties_opt);
		
		Option output_dir_opt = new Option("o", "output-dir", true, 
				"If specified, save BZipped Solr JSON files to this directory");
		output_dir_opt.setRequired(false);
		options.addOption(output_dir_opt);
		
		Option solr_base_url_opt = new Option("u", "solr-base-url", true, 
				"If specified, the base URL to post the Solr JSON data to");
		solr_base_url_opt.setRequired(false);
		options.addOption(solr_base_url_opt);
		
		Option read_only_opt = new Option("r", "read-only", false, 
				"Used to initiate a run where the files are all read in, but nothing is ingested/saved");
		read_only_opt.setRequired(false);
		options.addOption(read_only_opt);
		
		// Need to work with CLI v1.2 as this is the JAR that is bundled with Hadoop/Spark 
		CommandLineParser parser = new GnuParser(); 
		//CommandLineParser parser = new DefaultParser(); // if working with CLI v1.3 and above
		
		HelpFormatter formatter = new HelpFormatter();
		CommandLine cmd = null;

		try {
			cmd = parser.parse(options, args);
		}
		catch (ParseException e) {
			System.err.println(e.getMessage());
			print_usage(formatter,options);
			System.exit(1);
		}

		
		String verbosity_str = cmd.getOptionValue("verbosity","1");
		int verbosity = Integer.parseInt(verbosity_str);

		String property_filename = cmd.getOptionValue("properties",null);
	
		String output_dir = cmd.getOptionValue("output-dir",null);
		String solr_base_url   = cmd.getOptionValue("solr-base-url",null);
		boolean read_only   = cmd.hasOption("read-only");
		
		String[] filtered_args = cmd.getArgs();

		if (filtered_args.length != 2) {
			print_usage(formatter,options);
			System.exit(1);
		}
	
		if (property_filename != null) {
			try {
				FileInputStream fis = new FileInputStream(property_filename);
				BufferedInputStream bis = new BufferedInputStream(fis);
				
				System.getProperties().load(bis);
			} 
			catch (FileNotFoundException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
				System.err.println("File not found: '" + property_filename + "'. Skipping property file read");
			}
			catch (IOException e) {
				System.err.println("IO Exception for: '" + property_filename + "'. Malformed syntax? Skipping property file read");
			}
		}
		
		if (!read_only && ((output_dir == null) && (solr_base_url==null))) {
			System.err.println("Need to specify either --solr-base-url or --output-dir otherwise generated files are not ingested/saved");
			print_usage(formatter,options);
			System.exit(1);
		}
		if (read_only) {
			// For this case, need to ensure solr-url and output-dir are null
			output_dir = null;
			solr_base_url = null;
		}
		
		String input_file  = filtered_args[0];
		String solr_collection = filtered_args[1];
		
		ProcessForSerialSolrIngest prep_for_ingest 
			= new ProcessForSerialSolrIngest(input_file,solr_collection,solr_base_url,output_dir,verbosity);
			
		prep_for_ingest.execPerVolumeSequenceFile();
		
	}
}
