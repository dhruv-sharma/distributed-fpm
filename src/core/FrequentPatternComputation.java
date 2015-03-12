package core;

import java.io.IOException;
import java.util.List;

import org.apache.giraph.edge.Edge;
import org.apache.giraph.graph.BasicComputation;
import org.apache.giraph.graph.Vertex;
import org.apache.giraph.utils.MemoryUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;

/**
 * This class implements the vertex for the Gather Apply Scatter model to be
 * used for distributed frequent pattern mining. Each vertex is basically an
 * item in the database and edges represent the items which come together in
 * transactions.
 * 
 * @author Dhruv Sharma, Metacube Software Pvt. Ltd.
 *
 */

public class FrequentPatternComputation
		extends
		BasicComputation<IntWritable, ItemVertexValue, NullWritable, FrequentPatternMessage> {

	@Override
	public void compute(
			Vertex<IntWritable, ItemVertexValue, NullWritable> vertex,
			Iterable<FrequentPatternMessage> messages) throws IOException {

		/**
		 * Getting the minimum support value for the configuration.
		 */
		int MIN_SUPPORT = this.getConf().getInt(
				CommonConstants.MINIMUM_SUPPORT_STRING,
				CommonConstants.DEFAULT_MIN_SUPPORT);

		/**
		 * Messages for debugging purpose.
		 */
		System.out.println("Superstep : " + getSuperstep() + " Vertex : "
				+ vertex.getId().get() + " "
				+ MemoryUtils.getRuntimeMemoryStats());

		/**
		 * Getting the vertex id of the vertex for which the compute method is
		 * called.
		 */
		int vertexId = vertex.getId().get();

		/**
		 * Getting the vertex value of the vertex on which the compute method is
		 * called.
		 */
		ItemVertexValue vertexValue = vertex.getValue();

		/**
		 * Getting the current superstep number. The supersteps start from 0. In
		 * the 0th superstep all the vertices are active.
		 * 
		 */
		long currentSuperstep = this.getSuperstep();

		/**
		 * Creating a frequent pattern message which will be populated during
		 * the superstep and forwarded to neighbor vertices at the end of this
		 * superstep.
		 */
		FrequentPatternMessage messageToPropagate = null;

		if (currentSuperstep == 0) {
			/*****************************************************/
			/* Zeroth Super-Step Processing */
			/*****************************************************/
			/**
			 * If the item occurs in lesser number of transactions than the
			 * minimum support then it cannot be a part of any frequent itemset.
			 * We should set it's propagation status as false and vote to halt.
			 * 
			 * 
			 * Once, the propagation status is set to false the vertex will
			 * never send messages even if it becomes active at a later stage.
			 * 
			 * Otherwise, if the item occurs in equal (or more) transactions
			 * than the minimum specified support then it needs to send a
			 * message to it's neighboring vertices.
			 * 
			 */
			if (vertexValue.getTransactionIds().length < MIN_SUPPORT) {

				/**
				 * This vertex should not propagate any messages since this
				 * vertex itself is not frequent. Hence, setting the propagation
				 * status as false.
				 */
				vertexValue.setPropagationStatus(false);

				/**
				 * Remove this vertex from the graph.
				 */
				this.removeVertexRequest(vertex.getId());

				/**
				 * Remove all the outgoing edges of this vertex from this graph.
				 */
				for (Edge<IntWritable, NullWritable> edge : vertex.getEdges()) {
					// this.removeEdgesRequest(vertex.getId(),
					// edge.getTargetVertexId());
					vertex.removeEdges(edge.getTargetVertexId());
				}
			} else {

				/**
				 * List of transactions this item belongs to.
				 */
				int[] txnIdList = vertexValue.getTransactionIds();

				/**
				 * Create a message object to propagate further. This message
				 * will simply contain a single item and transaction list pair
				 * (itself and it's own transaction list).
				 */
				messageToPropagate = createFirstMessage(vertexId, txnIdList);

			}
		} else {
			/*****************************************************/
			/* Non-Zeroth Super-Step Processing */
			/*****************************************************/
			if (vertexValue.shouldPropagate()) {
				/**
				 * List of transactions this item belongs to.
				 */
				int[] currentVertexTransactions = vertexValue
						.getTransactionIds();

				/**
				 * Create a message object to propagate further if required.
				 */
				messageToPropagate = new FrequentPatternMessage();

				/**
				 * Iterate over each incoming message and find the overlap of
				 * the transaction list with the current items (vertices)
				 * transactions.
				 */
				for (FrequentPatternMessage currentIncomingMessage : messages) {

					/**
					 * Getting the list of items and transactions pairs which
					 * are the part of the current incoming message.
					 */
					List<int[]> vertexIdArrayList = currentIncomingMessage
							.getVertexIds();
					List<int[]> transactionIdArrayList = currentIncomingMessage
							.getTransactionIds();

					/**
					 * Iterating over each pair of items and transactions
					 * extracted.
					 */
					for (int i = 0; i < transactionIdArrayList.size(); i++) {

						int[] vertices = vertexIdArrayList.get(i);
						int[] transactions = transactionIdArrayList.get(i);

						/**
						 * Find Overlap between the incoming messages and the
						 * current vertices transactions.
						 */
						int overlapSize = getOverlapSize(
								currentVertexTransactions, transactions);

						if (overlapSize >= MIN_SUPPORT
								&& vertices.length == currentSuperstep) {

							int[] overlappingTransactions = getOverlap(
									currentVertexTransactions, transactions,
									overlapSize);

							int[] messageVertices = new int[vertices.length + 1];

							for (int k = 0; k < vertices.length; k++) {
								messageVertices[k] = vertices[k];
							}

							messageVertices[vertices.length] = vertexId;

							messageToPropagate.addFrequentPattern(
									messageVertices, overlappingTransactions);

							vertexValue.addFrequentPattern(messageVertices,
									overlappingTransactions);

						} else {

							/**
							 * Remove the edge from the graph.
							 */
							if (currentSuperstep == 1) {
								IntWritable sourceId = new IntWritable(
										vertices[0]);
								this.removeEdgesRequest(sourceId,
										vertex.getId());
							}
						}
					}
				}
			}
		}

		/**
		 * Send this message only in case the message is not empty. If the
		 * message is empty there is no sense in forwarding it.
		 */
		if (messageToPropagate != null && !messageToPropagate.isEmpty()) {
			this.sendMessageToAllEdges(vertex, messageToPropagate);
		} else {
			vertex.getValue().setPropagationStatus(false);
		}
		/**
		 * Halt the vertex after computation so that it is not active during the
		 * next super-step.
		 */
		vertex.voteToHalt();
	}

	public static FrequentPatternMessage createFirstMessage(int vertexId,
			int[] txnIdList) {
		FrequentPatternMessage firstMessage = new FrequentPatternMessage();
		int[] vertexIds = { vertexId };
		firstMessage.addFrequentPattern(vertexIds, txnIdList);
		return firstMessage;
	}

	public static int getOverlapSize(int[] a, int[] b) {
		int index1 = 0;
		int index2 = 0;
		int newSize = 0;
		while (true) {
			if (a[index1] < b[index2]) {
				index1++;
			} else if (a[index1] > b[index2]) {
				index2++;
			} else {
				newSize++;
				index1++;
				index2++;
			}
			if (index1 >= a.length || index2 >= b.length) {
				break;
			}
		}
		return newSize;
	}

	public static int[] getOverlap(int[] a, int[] b, int overlapSize) {

		int index1 = 0;
		int index2 = 0;
		int[] overlap = new int[overlapSize];
		int overlapIndex = 0;

		while (true) {
			if (a[index1] < b[index2]) {
				index1++;
			} else if (a[index1] > b[index2]) {
				index2++;
			} else {
				overlap[overlapIndex] = a[index1];
				overlapIndex++;
				index1++;
				index2++;
			}
			if (index1 >= a.length || index2 >= b.length) {
				break;
			}
		}

		return overlap;
	}
}
