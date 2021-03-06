package core;

import java.io.IOException;

import io.ItemVertexInputFormat;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.PosixParser;
import org.apache.giraph.conf.GiraphConfiguration;
import org.apache.giraph.io.formats.GiraphTextInputFormat;
import org.apache.giraph.io.formats.IdWithValueTextOutputFormat;
import org.apache.giraph.job.GiraphJob;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * 
 * @author Dhruv Sharma, Metacube Software Pvt. Ltd.
 * 
 */
public class MainClass {

	public static void main(String[] args) throws IOException,
			ClassNotFoundException, InterruptedException, ParseException {

		Options options = new Options();
		options.addOption("i", "input file", true, "Input data file");
		options.addOption("o", "output", true, "Output file");
		options.addOption("wmin", true, "Minimum number of workers");
		options.addOption("wmax", true, "Maximum number of workers");
		options.addOption("l", true, "Local test mode flag");
		options.addOption("minsup", true, "Minimum support value");
		options.addOption("maxsupersteps", true, "Maximum number of supersteps");

		CommandLineParser parser = new PosixParser();
		CommandLine cmd = parser.parse(options, args);

		GiraphConfiguration fpMiningJobConf = new GiraphConfiguration();

		fpMiningJobConf.setComputationClass(FrequentPatternComputation.class);

		fpMiningJobConf.setVertexInputFormatClass(ItemVertexInputFormat.class);

		fpMiningJobConf
				.setVertexOutputFormatClass(IdWithValueTextOutputFormat.class);

		fpMiningJobConf.setMaxNumberOfSupersteps(Integer.parseInt(cmd
				.getOptionValue("maxsupersteps")));

		// fpMiningJobConf.setLocalTestMode(true);
		fpMiningJobConf.setLocalTestMode(Boolean.parseBoolean(cmd
				.getOptionValue("l")));

		// fpMiningJobConf.setWorkerConfiguration(1, 1, 10.0f);
		fpMiningJobConf.setWorkerConfiguration(
				Integer.parseInt(cmd.getOptionValue("wmin")),
				Integer.parseInt(cmd.getOptionValue("wmax")), 100.0f);

		// fpMiningJobConf.setBoolean("giraph.SplitMasterWorker", false);
		fpMiningJobConf.setBoolean("giraph.SplitMasterWorker", true);

		fpMiningJobConf.setBoolean("giraph.useMessageSizeEncoding", true);

		fpMiningJobConf.setBoolean("giraph.useOutOfCoreGraph", true);

		fpMiningJobConf.setBoolean("giraph.useOutOfCoreMessage", true);

		fpMiningJobConf.setBoolean("giraph.isStaticGraph", true);

		fpMiningJobConf.setInt("giraph.yarn.task.heap.mb", 2048);

		fpMiningJobConf.setCheckpointFrequency(1);

		fpMiningJobConf.setMaxTaskAttempts(100);

		System.out.println("***** Use Checkpointing: "
				+ fpMiningJobConf.useCheckpointing());

		fpMiningJobConf.setInt(CommonConstants.MINIMUM_CUPPORT_STRING,
				Integer.parseInt(cmd.getOptionValue("minsup")));

		GiraphJob fpMiningJob = new GiraphJob(fpMiningJobConf,
				CommonConstants.FP_MINING_ALGOTIHHM_NAME);

		GiraphTextInputFormat.addVertexInputPath(fpMiningJobConf,
				new Path(cmd.getOptionValue('i')));

		FileOutputFormat.setOutputPath(fpMiningJob.getInternalJob(), new Path(
				cmd.getOptionValue('o')));

		fpMiningJob.run(true);
	}
}
