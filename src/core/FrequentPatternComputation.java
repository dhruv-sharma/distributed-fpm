package core;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import org.apache.giraph.graph.BasicComputation;
import org.apache.giraph.graph.Vertex;
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

	private static int DEFAULT_MIN_SUPPORT = 100;

	@Override
	public void compute(
			Vertex<IntWritable, ItemVertexValue, NullWritable> vertex,
			Iterable<FrequentPatternMessage> messages) throws IOException {

		/**
		 * Getting the minimum support value for the configuration.
		 */
		int MIN_SUPPORT = this.getConf().getInt(
				CommonConstants.MINIMUM_CUPPORT_STRING, DEFAULT_MIN_SUPPORT);
		/**
		 * Messages for debugging purpose.
		 */
		System.out.println("******************************************");
		System.out.println("Superstep : " + getSuperstep() + " Vertex : "
				+ vertex.getId().get());

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
		FrequentPatternMessage messageToPropogate = new FrequentPatternMessage();

		System.out.println("Vertex State Start: " + vertexValue.toString());

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
			if (vertexValue.getTransactionList().size() < MIN_SUPPORT) {
				vertexValue.setPropagationStatus(false);
				/**
				 * Remove this vertex from the graph.
				 */
				// this.removeVertexRequest(vertex.getId());
			} else {

				/**
				 * List of transactions this item belongs to.
				 */
				Set<Integer> txnIdList = vertexValue.getTransactionIdList();

				/**
				 * Create a message object to propagate further. This message
				 * will simply contain a single item and transaction list pair
				 * (itself and it's own transaction list).
				 */
				messageToPropogate = createFirstMessage(vertexId, txnIdList);

				/**
				 * Updating the item's (vertice's) own frequent pattern list.
				 * Since this item is frequent on it's own.
				 */
				vertexValue.addFrequentPatterns(messageToPropogate
						.getItemsAndTxns());

				/**
				 * Sending the new messages to all the outgoing edges of the
				 * current vertex.
				 */
				// this.sendMessageToAllEdges(vertex, messageToPropogate);
				// this.sendMessageToRelevantNeighbors(vertex, toForward);
			}
		} else {
			/*****************************************************/
			/* Non-Zeroth Super-Step Processing */
			/*****************************************************/
			if (vertexValue.shouldPropagate()) {
				/**
				 * List of transactions this item belongs to.
				 */
				Set<Integer> currentVertexTxnIdList = vertexValue
						.getTransactionIdList();

				/**
				 * Create a message object to propagate further if required.
				 */
				messageToPropogate = new FrequentPatternMessage();

				/**
				 * Iterate over each incoming message and find the overlap of
				 * the transaction list with the current items (vertices)
				 * transactions.
				 */
				for (FrequentPatternMessage currentIncomingMessage : messages) {

					/**
					 * Message for debugging purpose.
					 */
					System.out.println("Incoming Message: "
							+ currentIncomingMessage);

					/**
					 * Getting the list of items and transactions pairs which
					 * are the part of the current incoming message.
					 */
					Set<ItemsAndTransactionsPair> list = currentIncomingMessage
							.getItemsAndTxns();

					/**
					 * Iterating over each pair of items and transactions
					 * extracted.
					 */
					for (ItemsAndTransactionsPair pair : list) {

						/**
						 * Find Overlap between the incoming messages and the
						 * current vertices transactions.
						 */
						Set<Integer> currentMessagePairTxnIdList = pair
								.getTransactionIds();

						/**
						 * Finding the overlap between the current item's
						 * transaction list and the current items and
						 * transactions pair's transaction list.
						 */
						Set<Integer> overlappingTxnIdList = getOverlappingList(
								currentVertexTxnIdList,
								currentMessagePairTxnIdList);

						/**
						 * If the cardinality of the overlapping set between
						 * them is greater than or equal to the minimum support
						 * then they are a frequent itemset.
						 */
						if (overlappingTxnIdList.size() >= MIN_SUPPORT) {

							Set<Integer> msgItemIdList = pair.getVertexIds();

							/**
							 * Add Vertices and Transaction Pairs to the message
							 * to forward.
							 */
							if (msgItemIdList.size() == currentSuperstep) {
								/**
								 * adding the vertex id to the list of item ids.
								 */
								msgItemIdList.add(vertexId);

								ItemsAndTransactionsPair itemsAndTxnsPairToAdd = new ItemsAndTransactionsPair(
										msgItemIdList, overlappingTxnIdList);

								messageToPropogate
										.addItemAndTransactionPair(itemsAndTxnsPairToAdd);
							}
						}
					}
				}

				vertexValue.addFrequentPatterns(messageToPropogate
						.getItemsAndTxns());

				System.out.println("Vertex State End: "
						+ vertexValue.toString());

			}

		}

		System.out.println("******************************************");

		/**
		 * Send this message only in case the message is not empty. If the
		 * message is empty there is no sense in forwarding it.
		 */
		if (!messageToPropogate.isEmpty()) {
			this.sendMessageToAllEdges(vertex, messageToPropogate);
		}

		/**
		 * Halt the vertex after computation so that it is not active during the
		 * next super-step.
		 */
		vertex.voteToHalt();
	}

	public static FrequentPatternMessage createFirstMessage(int vertexId,
			Set<Integer> txnIdList) {
		FrequentPatternMessage firstMessage = new FrequentPatternMessage();

		Set<Integer> selfIdSet = new HashSet<Integer>();
		selfIdSet.add(vertexId);
		ItemsAndTransactionsPair pair = new ItemsAndTransactionsPair(selfIdSet,
				txnIdList);
		firstMessage.addItemAndTransactionPair(pair);
		return firstMessage;
	}

	public static Set<Integer> getOverlappingList(Set<Integer> list1,
			Set<Integer> list2) {
		Set<Integer> overlappingList = new HashSet<Integer>();
		Set<Integer> toIterate;
		Set<Integer> toCheck;
		if (list1.size() < list2.size()) {
			toIterate = list1;
			toCheck = list2;
		} else {
			toIterate = list2;
			toCheck = list1;
		}
		for (Integer id : toIterate) {
			if (toCheck.contains(id)) {
				overlappingList.add(id);
			}
		}
		return overlappingList;
	}

}
