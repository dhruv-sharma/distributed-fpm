package core;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.io.Writable;
import org.json.JSONArray;
import org.json.JSONException;

/**
 * 
 * @author Dhruv Sharma, Metacube Software Pvt. Ltd.
 * 
 */
public class ItemVertexValue implements Writable {

	private static int SHOULD_PROPOGATE_INDEX = 0;

	private static int TRANSACTION_LIST_INDEX = 1;

	private static int FREQUENT_PATTERNS_LIST_INDEX = 2;

	private Set<Transaction> transactionList = new HashSet<Transaction>();

	private boolean shouldPropagate = true;

	private Set<ItemsAndTransactionsPair> frequentPatters = new HashSet<ItemsAndTransactionsPair>();

	public void addFrequentPattern(ItemsAndTransactionsPair pair) {
		this.frequentPatters.add(pair);
	}

	public void addFrequentPatterns(Set<ItemsAndTransactionsPair> pair) {
		this.frequentPatters.addAll(pair);
	}

	public Set<ItemsAndTransactionsPair> getFrequentPatters() {
		return frequentPatters;
	}

	public void setFrequentPatters(Set<ItemsAndTransactionsPair> frequentPatters) {
		this.frequentPatters = frequentPatters;
	}

	public Set<Transaction> getTransactionList() {
		return transactionList;
	}

	public void setPropagationStatus(boolean ps) {
		this.shouldPropagate = ps;
	}

	public boolean shouldPropagate() {
		return this.shouldPropagate;
	}

	public Set<Integer> getTransactionIdList() {
		Set<Integer> idList = new HashSet<Integer>();
		for (Transaction txn : this.transactionList) {
			idList.add(txn.getId());
		}
		return idList;
	}

	public void setTransactionList(Set<Transaction> transactionList) {
		this.transactionList = transactionList;
	}

	public boolean addTransaction(Transaction txn) {
		return this.transactionList.add(txn);
	}

	@Override
	public void readFields(DataInput dataInput) throws IOException {
		String inputLine = dataInput.readUTF();
		try {

			/**
			 * Reading the input line into a JSONArray object.
			 */
			JSONArray itemVertexValueJSONArray = new JSONArray(inputLine);

			/**
			 * Setting Propagation status from the JSON Array read through the
			 * input text line.
			 */
			this.shouldPropagate = itemVertexValueJSONArray
					.getBoolean(SHOULD_PROPOGATE_INDEX);

			/**
			 * Setting the transaction list from the JSON Array read through the
			 * input text line.
			 */
			JSONArray txnListJSONArray = itemVertexValueJSONArray
					.getJSONArray(TRANSACTION_LIST_INDEX);
			for (int i = 0; i < txnListJSONArray.length(); i++) {
				this.transactionList.add(new Transaction(txnListJSONArray
						.getInt(i)));
			}

			/**
			 * Setting the frequent pattern list from the JSON Array read
			 * through the input text line.
			 */

			String frequentPatternListStringFormat = itemVertexValueJSONArray
					.getString(FREQUENT_PATTERNS_LIST_INDEX);

			JSONArray frequentPatternListJSONArray = new JSONArray(
					frequentPatternListStringFormat);

			for (int j = 0; j < frequentPatternListJSONArray.length(); j++) {
				JSONArray fpJSONObject = frequentPatternListJSONArray
						.getJSONArray(j);
				JSONArray vertexArray = fpJSONObject
						.getJSONArray(ItemsAndTransactionsPair.VERTEX_ID_INDEX);
				JSONArray transactionArray = fpJSONObject
						.getJSONArray(ItemsAndTransactionsPair.TRANSACTION_ID_INDEX);
				Set<Integer> vertexSet = new HashSet<Integer>(
						vertexArray.length());
				for (int i = 0; i < vertexArray.length(); i++) {
					vertexSet.add(vertexArray.getInt(i));
				}
				Set<Integer> transactionSet = new HashSet<Integer>(
						transactionArray.length());
				for (int k = 0; k < transactionArray.length(); k++) {
					transactionSet.add(transactionArray.getInt(k));
				}
				ItemsAndTransactionsPair pair = new ItemsAndTransactionsPair(
						vertexSet, transactionSet);
				this.frequentPatters.add(pair);
			}
		} catch (JSONException e) {
			e.printStackTrace();
		}
	}

	@Override
	public void write(DataOutput dataOut) throws IOException {
		JSONArray itemVertexValueJSONArray = new JSONArray();
		try {
			itemVertexValueJSONArray.put(SHOULD_PROPOGATE_INDEX,
					this.shouldPropagate);
			itemVertexValueJSONArray.put(TRANSACTION_LIST_INDEX,
					this.transactionList);
			itemVertexValueJSONArray.put(FREQUENT_PATTERNS_LIST_INDEX,
					this.frequentPatters);
		} catch (JSONException e) {
			e.printStackTrace();
		}
		dataOut.writeUTF(itemVertexValueJSONArray.toString());
	}

	@Override
	public String toString() {
		StringBuffer buf = new StringBuffer();
		JSONArray itemVertexValueJSONArray = new JSONArray();
		try {
			itemVertexValueJSONArray.put(SHOULD_PROPOGATE_INDEX,
					this.shouldPropagate);
			itemVertexValueJSONArray.put(TRANSACTION_LIST_INDEX,
					this.transactionList);
			itemVertexValueJSONArray.put(FREQUENT_PATTERNS_LIST_INDEX,
					this.frequentPatters);
		} catch (JSONException e) {
			e.printStackTrace();
		}
		buf.append(itemVertexValueJSONArray.toString());
		return buf.toString();
	}

}
