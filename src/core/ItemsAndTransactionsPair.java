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
 */
public class ItemsAndTransactionsPair implements Writable {

	public static int VERTEX_ID_INDEX = 0;

	public static int TRANSACTION_ID_INDEX = 1;

	private Set<Integer> vertexIds = new HashSet<Integer>();

	private Set<Integer> transactionIds = new HashSet<Integer>();

	public ItemsAndTransactionsPair(Set<Integer> vertexIds,
			Set<Integer> transactionIds) {
		super();
		this.vertexIds = vertexIds;
		this.transactionIds = transactionIds;
	}

	public Set<Integer> getVertexIds() {
		return vertexIds;
	}

	public void setVertexIds(Set<Integer> vertexIds) {
		this.vertexIds = vertexIds;
	}

	public Set<Integer> getTransactionIds() {
		return transactionIds;
	}

	public void setTransactionIds(Set<Integer> transactionIds) {
		this.transactionIds = transactionIds;
	}

	@Override
	public String toString() {
		StringBuffer buf = new StringBuffer();
		JSONArray itemsandTransactionsArray = new JSONArray();
		itemsandTransactionsArray.put(this.vertexIds);
		itemsandTransactionsArray.put(this.transactionIds);
		buf.append(itemsandTransactionsArray.toString());
		return buf.toString();
	}

	@Override
	public void readFields(DataInput dataIn) throws IOException {
		String line = dataIn.readUTF();
		try {
			JSONArray itemsAndTransactionsArray = new JSONArray(line);
			JSONArray itemsArray = new JSONArray(
					itemsAndTransactionsArray.getJSONArray(VERTEX_ID_INDEX));
			JSONArray transactionsArray = new JSONArray(
					itemsAndTransactionsArray
							.getJSONArray(TRANSACTION_ID_INDEX));
			for (int i = 0; i < itemsArray.length(); i++) {
				vertexIds.add(itemsArray.getInt(i));
			}
			for (int j = 0; j < transactionsArray.length(); j++) {
				transactionIds.add(transactionsArray.getInt(j));
			}
		} catch (JSONException e) {
			e.printStackTrace();
		}

	}

	@Override
	public void write(DataOutput dataOut) throws IOException {
		JSONArray itemsandTransactionsArray = new JSONArray();
		try {
			itemsandTransactionsArray.put(VERTEX_ID_INDEX, this.vertexIds);
			itemsandTransactionsArray.put(TRANSACTION_ID_INDEX,
					this.transactionIds);
		} catch (JSONException e) {
			e.printStackTrace();
		}
		dataOut.writeUTF(itemsandTransactionsArray.toString());
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result
				+ ((transactionIds == null) ? 0 : transactionIds.hashCode());
		result = prime * result
				+ ((vertexIds == null) ? 0 : vertexIds.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		ItemsAndTransactionsPair other = (ItemsAndTransactionsPair) obj;
		if (transactionIds == null) {
			if (other.transactionIds != null)
				return false;
		} else if (!transactionIds.equals(other.transactionIds))
			return false;
		if (vertexIds == null) {
			if (other.vertexIds != null)
				return false;
		} else if (!vertexIds.equals(other.vertexIds))
			return false;
		return true;
	}

}
