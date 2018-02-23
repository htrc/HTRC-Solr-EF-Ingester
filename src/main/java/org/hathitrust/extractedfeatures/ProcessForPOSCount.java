package org.hathitrust.extractedfeatures;

import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.Serializable;
import org.apache.commons.cli.*;

import org.apache.spark.api.java.*;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.util.DoubleAccumulator;
import scala.Tuple2;

import org.apache.spark.SparkConf;

public class ProcessForPOSCount implements Serializable
{
	private static final long serialVersionUID = 1L;

	// Following details on number of partitions to use given in 
	//  "Parallelized collections" section of:
	//   https://spark.apache.org/docs/2.0.1/programming-guide.html
	//
	// For a more detailed discussion see:
	//   http://blog.cloudera.com/blog/2015/03/how-to-tune-your-apache-spark-jobs-part-2/
	
	//protected static final int DEFAULT_NUM_CORES = 6;
	//protected static final int DEFAULT_NUM_PARTITIONS = 3*DEFAULT_NUM_CORES; 
	protected static final int DEFAULT_FILES_PER_PARTITION = 3000;
	
	protected String _input_dir;
	protected String _json_list_filename;
	
	protected int    _verbosity;

	public ProcessForPOSCount(String input_dir, String json_list_filename, int verbosity)
	{
		_input_dir = input_dir;
		_json_list_filename = (json_list_filename != null) ? json_list_filename : input_dir;

		_verbosity  = verbosity;
	}

	protected String generateSparkAppName(String exec_mode)
	{
		String spark_app_name = "[" + exec_mode + "] Extracted Features: Process for POS Count";
		spark_app_name += " [" + _json_list_filename + "]";
	
		return spark_app_name;
	}
		
	public void execPOSCount()
	{	
		String spark_app_name = generateSparkAppName("Per Volume");		
		
		SparkConf conf = new SparkConf().setAppName(spark_app_name);
		JavaSparkContext jsc = new JavaSparkContext(conf);

		String filename_root = _json_list_filename.replaceAll(".*/","").replaceAll("\\..*$","");
		String output_directory = "pos-" + filename_root + "-out";
		if (ClusterFileIO.exists(output_directory))
		{
			System.err.println("Error: " + output_directory + " already exists.  Spark unable to write output data");
			jsc.close();
			System.exit(1);
		}
		
		//int num_partitions = Integer.getInteger("wcsa-ef-ingest.num-partitions", DEFAULT_NUM_PARTITIONS);
		int files_per_partition = Integer.getInteger("wcsa-ef-ingest.num-partitions", DEFAULT_FILES_PER_PARTITION);
		
		
		JavaRDD<String> json_list_data = jsc.textFile(_json_list_filename).cache();
		json_list_data.setName("JSON-file-list");
		
		long num_volumes = json_list_data.count();
		double per_vol = 100.0/(double)num_volumes;

		int num_partitions = (int)(num_volumes/files_per_partition)+1;
		JavaRDD<String> json_list_data_rp = json_list_data.repartition(num_partitions);
		json_list_data_rp.setName("JSON-file-list--repartitioned");
		
		DoubleAccumulator per_vol_progress_accum = jsc.sc().doubleAccumulator("Per Volume Progress Percent");
		
		boolean strict_file_io = Boolean.getBoolean("wcsa-ef-ingest.strict-file-io");
		
		PerVolumePOSStreamFlatmap paged_solr_posfreq_flatmap 
			= new PerVolumePOSStreamFlatmap(_input_dir,_verbosity, 
								     per_vol_progress_accum,per_vol,
								     strict_file_io);
		JavaRDD<String> pos_list = json_list_data_rp.flatMap(paged_solr_posfreq_flatmap); 
		pos_list.setName("pos-stream");
		
		/*
		JavaPairRDD<String, Integer> pos_pairs = pos_list.mapToPair(new PairFunction<String, String, Integer>() {
			public Tuple2<String, Integer> call(String s) { return new Tuple2<String, Integer>(s, 1); }
		});
		pos_pairs.setName("single-pos-count");
		
		JavaPairRDD<String, Integer> pos_counts = pos_pairs.reduceByKey(new Function2<Integer, Integer, Integer>() {
			public Integer call(Integer a, Integer b) { return a + b; }
		});
		pos_counts.setName("pos-frequency");
		*/
		
		JavaPairRDD<String, Long> pos_pairs = pos_list.mapToPair(s -> new Tuple2<String, Long>(s, 1L));
		pos_pairs.setName("single-pos-count");
		
		JavaPairRDD<String, Long> pos_counts = pos_pairs.reduceByKey((a, b) -> a + b);
		pos_counts.setName("pos-frequency");
		
		JavaPairRDD<Long, String> pos_counts_swapped_pair
			= pos_counts.mapToPair(item -> item.swap());
		pos_counts_swapped_pair.setName("frequency-pos-swap");
		
		JavaPairRDD<Long, String> pos_counts_swapped_pair_sorted 
			= pos_counts_swapped_pair.sortByKey(false, num_partitions);
		pos_counts_swapped_pair_sorted.setName("descending-sorted-frequency-pos");
		
		JavaPairRDD<String, Long> pos_count_sorted 
			= pos_counts_swapped_pair_sorted.mapToPair(item -> item.swap());
		pos_count_sorted.setName("descending-pos-frequency");
		
		
		pos_count_sorted.saveAsTextFile(output_directory);
		jsc.close();
	}

	
	public static void print_usage(HelpFormatter formatter, Options options)
	{
		formatter.printHelp("RUN.bash [options] input-dir json-filelist.txt", options);
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
				e.printStackTrace();
				System.err.println("File not found: '" + property_filename + "'. Skipping property file read");
			}
			catch (IOException e) {
				System.err.println("IO Exception for: '" + property_filename + "'. Malformed syntax? Skipping property file read");
			}
		}
				
		String input_dir  = filtered_args[0];
		String json_list_filename = filtered_args[1];
		
		ProcessForPOSCount prep_for_pos 
			= new ProcessForPOSCount(input_dir,json_list_filename,verbosity);
		
		prep_for_pos.execPOSCount();
	
	}
}
