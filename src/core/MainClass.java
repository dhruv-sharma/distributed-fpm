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

	private static String FP_MINING_ALGOTIHHM_NAME = "Frequent Pattern Mining Algorithm";

	public static void main(String[] args) throws IOException,
			ClassNotFoundException, InterruptedException, ParseException {

		Options options = new Options();
		options.addOption("i", "input file", true, "Input data file");
		options.addOption("o", "output", true, "Output file");

		CommandLineParser parser = new PosixParser();
		CommandLine cmd = parser.parse(options, args);

		GiraphConfiguration fpMiningJobConf = new GiraphConfiguration();

		fpMiningJobConf.setComputationClass(FrequentPatternComputation.class);
		fpMiningJobConf.setVertexInputFormatClass(ItemVertexInputFormat.class);
		fpMiningJobConf
				.setVertexOutputFormatClass(IdWithValueTextOutputFormat.class);

		fpMiningJobConf.setMaxNumberOfSupersteps(5);

		fpMiningJobConf.setLocalTestMode(true);
		fpMiningJobConf.setWorkerConfiguration(1, 1, 10.0f);
		fpMiningJobConf.setBoolean("giraph.SplitMasterWorker", false);

		System.out.println("Value of property:  giraph.SplitMasterWorker = "
				+ fpMiningJobConf.getSplitMasterWorker());

		GiraphJob fpMiningJob = new GiraphJob(fpMiningJobConf,
				FP_MINING_ALGOTIHHM_NAME);

		GiraphTextInputFormat.addVertexInputPath(fpMiningJobConf,
				new Path(cmd.getOptionValue('i')));

		FileOutputFormat.setOutputPath(fpMiningJob.getInternalJob(), new Path(
				cmd.getOptionValue('o')));

		fpMiningJob.run(true);
	}
}
