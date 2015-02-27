package io;

import java.io.IOException;
import java.util.List;

import org.apache.giraph.edge.Edge;
import org.apache.giraph.edge.EdgeFactory;
import org.apache.giraph.graph.Vertex;
import org.apache.giraph.io.formats.TextVertexInputFormat;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.json.JSONArray;
import org.json.JSONException;

import com.google.common.collect.Lists;

import core.ItemVertexValue;
import core.Transaction;

/**
 * 
 * @author Dhruv Sharma, Metacube Software Pvt. Ltd.
 * 
 */

public class ItemVertexInputFormat extends
		TextVertexInputFormat<IntWritable, ItemVertexValue, NullWritable> {

	@Override
	public TextVertexReader createVertexReader(InputSplit split,
			TaskAttemptContext context) {
		return new JsonItemVertexReader();
	}

	/**
	 * VertexReader use for the Frequent Pattern Mining GAS Algorithm. The files
	 * should be in the following JSON format: JSONArray(<vertex id>,
	 * 
	 * 
	 * Example line: [1, [2,3,4], [1,3,6]]
	 * 
	 * vertex with id = 1 has 2, 3 and 4 as neighbor vertices and is present in
	 * transactions with id's 1m 3 and 6.
	 * 
	 * index 0 - vertex id which is integer. index 1 - neighbor vertex id's
	 * which is a JSONArray. index 2 - transaction id's which is a JSONArray.
	 */
	class JsonItemVertexReader
			extends
			TextVertexReaderFromEachLineProcessedHandlingExceptions<JSONArray, JSONException> {

		@Override
		protected JSONArray preprocessLine(Text line) throws JSONException {
			return new JSONArray(line.toString());
		}

		@Override
		protected IntWritable getId(JSONArray jsonVertex) throws JSONException,
				IOException {
			System.out.println("Reading Vertex Id: " + jsonVertex.getInt(0));
			return new IntWritable(jsonVertex.getInt(0));
		}

		@Override
		protected ItemVertexValue getValue(JSONArray jsonVertex)
				throws JSONException, IOException {
			ItemVertexValue value = new ItemVertexValue();
			JSONArray jsonTransactionArray = jsonVertex.getJSONArray(2);
			for (int i = 0; i < jsonTransactionArray.length(); i++) {
				Transaction currentTransaction = new Transaction(
						jsonTransactionArray.getInt(i));
				value.addTransaction(currentTransaction);
			}
			System.out.println("Reading Vertex Value: " + value.toString());
			return value;
		}

		@Override
		protected Iterable<Edge<IntWritable, NullWritable>> getEdges(
				JSONArray jsonVertex) throws JSONException, IOException {

			JSONArray jsonEdgeArray = jsonVertex.getJSONArray(1);

			/* get the edges */
			List<Edge<IntWritable, NullWritable>> edges = Lists
					.newArrayListWithCapacity(jsonEdgeArray.length());

			NullWritable nullValue = NullWritable.get();

			for (int i = 0; i < jsonEdgeArray.length(); ++i) {
				IntWritable targetId;
				targetId = new IntWritable(jsonEdgeArray.getInt(i));
				edges.add(EdgeFactory.create(targetId, nullValue));

			}
			System.out.println("Edges: " + edges.toString());
			return edges;
		}

		@Override
		protected Vertex<IntWritable, ItemVertexValue, NullWritable> handleException(
				Text line, JSONArray jsonVertex, JSONException e) {

			throw new IllegalArgumentException("Couldn't get vertex from line "
					+ line, e);
		}

	}

}
