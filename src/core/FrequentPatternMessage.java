package core;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.io.Writable;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

/**
 * Message class implemented for sharing messages across supersteps for the
 * given implementation of frequent pattern mining algorithm.
 * 
 * @author Dhruv Sharma, Metacube Software Pvt. Ltd.
 * 
 */

public class FrequentPatternMessage implements Writable {

	private Set<ItemsAndTransactionsPair> itemsAndTxns = new HashSet<ItemsAndTransactionsPair>();

	public boolean isEmpty() {
		return this.itemsAndTxns.isEmpty();
	}

	public void addItemAndTransactionPair(ItemsAndTransactionsPair pair) {
		this.itemsAndTxns.add(pair);
	}

	public Set<ItemsAndTransactionsPair> getItemsAndTxns() {
		return itemsAndTxns;
	}

	public void setItemsAndTxns(Set<ItemsAndTransactionsPair> itemsAndTxns) {
		this.itemsAndTxns = itemsAndTxns;
	}

	private void clear() {
		this.itemsAndTxns.clear();
	}

	@Override
	public void readFields(DataInput dataIn) throws IOException {
		this.clear();
		String strData = dataIn.readUTF();
		try {
			JSONArray objectArray = new JSONArray(strData);
			JSONArray itemsAndTxnsArray = objectArray.getJSONArray(0);
			for (int i = 0; i < itemsAndTxnsArray.length(); i++) {
				JSONObject itemsAndTxnsPairObject = itemsAndTxnsArray
						.getJSONObject(i);
				JSONArray vertexArray = itemsAndTxnsPairObject
						.getJSONArray("vertexIds");
				JSONArray transactionArray = itemsAndTxnsPairObject
						.getJSONArray("transactionIds");
				Set<Integer> vertexSet = new HashSet<Integer>();
				for (int j = 0; j < vertexArray.length(); j++) {
					vertexSet.add(vertexArray.getInt(j));
				}
				Set<Integer> transactionSet = new HashSet<Integer>();
				for (int k = 0; k < transactionArray.length(); k++) {
					transactionSet.add(transactionArray.getInt(k));
				}
				ItemsAndTransactionsPair pair = new ItemsAndTransactionsPair(
						vertexSet, transactionSet);
				this.itemsAndTxns.add(pair);

			}
		} catch (JSONException e) {
			e.printStackTrace();
		}
	}

	@Override
	public void write(DataOutput dataOut) throws IOException {
		JSONArray itemsAndTxnsArray = new JSONArray();
		itemsAndTxnsArray.put(this.itemsAndTxns);
		dataOut.writeUTF(itemsAndTxnsArray.toString());
	}

	@Override
	public String toString() {
		StringBuffer buf = new StringBuffer();
		JSONArray itemsAndTxnsArray = new JSONArray();
		itemsAndTxnsArray.put(this.itemsAndTxns);
		buf.append(itemsAndTxnsArray.toString());
		return buf.toString();
	}

}
