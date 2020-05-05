package org.hathitrust.extractedfeatures;

import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.Serializable;
import java.util.Iterator;

import org.apache.commons.cli.*;
import org.apache.hadoop.io.Text;
import org.apache.spark.api.java.*;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.util.DoubleAccumulator;
import scala.Tuple2;

import org.apache.spark.SparkConf;

public class ProcessForWhitelist implements Serializable
{
	private static final long serialVersionUID = 1L;

	// Following details on number of partitions to use given in 
	//  "Parallelized collections" section of:
	//   https://spark.apache.org/docs/2.0.1/programming-guide.html
	//
	// For a more detailed discussion see:
	//   http://blog.cloudera.com/blog/2015/03/how-to-tune-your-apache-spark-jobs-part-2/
	
	protected static final int DEFAULT_NUM_CORES = 6;
	protected static final int DEFAULT_NUM_PARTITIONS = 3*DEFAULT_NUM_CORES; 
	
	// The above settings were changed in later Spark Main Programs to the following:
	//protected static final int DEFAULT_NUM_CORES = 10;
	//protected static final int MINIMUM_NUM_PARTITIONS = 10*DEFAULT_NUM_CORES; 
	
	//protected static final int DEFAULT_FILES_PER_PARTITION = 3000;
	
	
	protected String _input_dir;
	//protected String _json_list_filename;
	protected String _output_dir;
	
	protected int    _verbosity;

	public ProcessForWhitelist(String input_dir, String output_dir, int verbosity)
	{
		_input_dir = input_dir;
		//_json_list_filename = (json_list_filename != null) ? json_list_filename : input_dir;
		_output_dir = output_dir;
		
		_verbosity  = verbosity;
	}

	protected String generateSparkAppName(String exec_mode)
	{
		String spark_app_name = "[" + exec_mode + "] Extracted Features: Process for Whitelist";
		//spark_app_name += " [" + _json_list_filename + "]";

		if (_output_dir != null) { 
			spark_app_name += " output_dir=" + _output_dir;
		}
		
		return spark_app_name;
	}
		
		
	
	public void execWordCount()
	{	
		String spark_app_name = generateSparkAppName("Per Volume");	
		
		SparkConf conf = new SparkConf().setAppName(spark_app_name);
		JavaSparkContext jsc = new JavaSparkContext(conf);
		jsc.hadoopConfiguration().set("io.compression.codec.bzip2.library", "java-builtin");
		
		if (ClusterFileIO.exists(_output_dir))
		{
			System.err.println("Error: " + _output_dir + " already exists.  Spark unable to write output data");
			jsc.close();
			System.exit(1);
		}
		
		//JavaRDD<String> json_list_data = jsc.textFile(_json_list_filename,num_partitions).cache();
		//json_list_data.setName("JSON-file-list");
		
		//String packed_sequence_path = "hdfs:///user/capitanu/data/packed-ef-2.0"; 
		String packed_sequence_path = _input_dir;
		
		JavaPairRDD<Text, Text> input_pair_rdd = jsc.sequenceFile(packed_sequence_path, Text.class, Text.class);
		input_pair_rdd.setName("Sequence-file");
		
		// 'withReplacement' param described here:
		//   https://stackoverflow.com/questions/53689047/what-does-withreplacement-do-if-specified-for-sample-against-a-spark-dataframe
		// So setting to 'false' means a chosen item is only considered once.
		JavaPairRDD<Text, Text> input_pair_sampled_rdd = input_pair_rdd.sample(false,0.01,42); // 1%, seed-value=42

		JavaRDD<Text> json_text_rdd = input_pair_sampled_rdd.map(item -> item._2).cache(); // added cache() due to call to count() below
		// //JavaRDD<Text> json_text_rdd = input_pair_rdd.map(item -> new Text(item._2));
		
		// JavaRDD<Text> json_text_rdd = input_pair_rdd.map(item -> item._2).cache();
		
		
		int num_partitions = Integer.getInteger("wcsa-ef-ingest.num-partitions", DEFAULT_NUM_PARTITIONS);
		
	
		
		//long num_volumes = json_list_data.count();
		long num_volumes = json_text_rdd.count();
		double per_vol = 100.0/(double)num_volumes;
		
		//JavaRDD<String> json_list_data_rp = json_list_data.repartition((int)(num_volumes/100));

		DoubleAccumulator per_vol_progress_accum = jsc.sc().doubleAccumulator("Per Volume Progress Percent");
		
		boolean icu_tokenize = Boolean.getBoolean("wcsa-ef-ingest.icu-tokenize");
		boolean strict_file_io = Boolean.getBoolean("wcsa-ef-ingest.strict-file-io");
		
		//System.err.println("***** icu_tokenize = " + icu_tokenize);
		//System.err.println("***** num_part = " + num_partitions);

		PerVolumeWordStreamFlatmap paged_solr_wordfreq_flatmap 
			= new PerVolumeWordStreamFlatmap(_input_dir,_verbosity, 
								     per_vol_progress_accum,per_vol,
								     icu_tokenize,
								     strict_file_io);
		
		//JavaRDD<String> words = json_list_data.flatMap(paged_solr_wordfreq_map); 
		JavaRDD<String> words = json_text_rdd.flatMap(paged_solr_wordfreq_flatmap); 
		words.setName("tokenized-words");
		
		JavaPairRDD<String, Long> pairs = words.mapToPair(new PairFunction<String, String, Long>() {
			public Tuple2<String, Long> call(String s) { return new Tuple2<String, Long>(s, 1L); }
		});
		pairs.setName("single-word-count");
		
		JavaPairRDD<String, Long> counts = pairs.reduceByKey(new Function2<Long, Long, Long>() {
			public Long call(Long a, Long b) { return a + b; }
		});
		counts.setName("word-frequency");
		
		/*
		JavaPairRDD<Long, String> swapped_pair = counts.mapToPair(new PairFunction<Tuple2<String, Long>, Long, String>() {
	           @Override
	           public Tuple2<Long, String> call(Tuple2<String, Long> item) throws Exception {
	               return item.swap();
	           }

	        });
		swapped_pair.setName("frequency-word-swap");
		
		JavaPairRDD<Long, String> sorted_swapped_pair = swapped_pair.sortByKey(false,num_partitions);
		sorted_swapped_pair.setName("descending-sorted-frequency-word");
		
		JavaPairRDD<String, Long> sorted_swaped_back_pair = sorted_swapped_pair.mapToPair(new PairFunction<Tuple2<Long, String>, String, Long>() {
	           @Override
	           public Tuple2<String, Long> call(Tuple2<Long, String> item) throws Exception {
	               return item.swap();
	           }
	        });
		sorted_swaped_back_pair.setName("descending-word-frequency");
		*/
		

		JavaPairRDD<Long, String> counts_swapped_pair
			= counts.mapToPair(item -> item.swap());
		counts_swapped_pair.setName("frequency-word-swap");
		
		JavaPairRDD<Long, String> counts_swapped_pair_sorted 
			= counts_swapped_pair.sortByKey(false, num_partitions);
		counts_swapped_pair_sorted.setName("descending-sorted-frequency-word");
		
		JavaPairRDD<String, Long> count_sorted 
			= counts_swapped_pair_sorted.mapToPair(item -> item.swap());
		count_sorted.setName("descending-word-frequency");
		
		
		
		//sorted_swaped_back_pair.saveAsTextFile(output_directory);
		count_sorted.saveAsTextFile(_output_dir);
		
		
		//System.out.println("");
		//System.out.println("############");
		//System.out.println("# Number of page ids: " + num_page_ids);
		//System.out.println("############");
		
		jsc.close();
	}

	
	public static void print_usage(HelpFormatter formatter, Options options)
	{
		formatter.printHelp("RUN.sh [options] input-dir", options);
	}
	
	public static void main(String[] args) {
		Options options = new Options();

		Option output_dir_opt = new Option("o", "output-dir", true,
				"Specify the directory where the word-counts are saved to");
		output_dir_opt.setRequired(true);
		options.addOption(output_dir_opt);
		
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

		String output_dir = cmd.getOptionValue("output-dir",null);
		
		String verbosity_str = cmd.getOptionValue("verbosity","1");
		int verbosity = Integer.parseInt(verbosity_str);

		String property_filename = cmd.getOptionValue("properties",null);
		
		String[] filtered_args = cmd.getArgs();

		if (filtered_args.length != 1) {
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
		
		ProcessForWhitelist prep_for_whitelist 
			= new ProcessForWhitelist(input_dir,output_dir,verbosity);
		
		prep_for_whitelist.execWordCount();
	
	}
}
