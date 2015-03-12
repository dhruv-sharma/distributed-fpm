package core;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

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

	private static int TRANSACTION_IDS_LIST_INDEX = 1;

	private static int VERTEX_IDS_LIST_INDEX = 2;

	private List<int[]> vertexIds = new ArrayList<int[]>();

	private List<int[]> transactionIds = new ArrayList<int[]>();

	private boolean shouldPropagate = true;

	public List<int[]> getVertexIds() {
		return vertexIds;
	}

	public void setVertexIds(List<int[]> vertexIds) {
		this.vertexIds = vertexIds;
	}

	public boolean isShouldPropagate() {
		return shouldPropagate;
	}

	public void setShouldPropagate(boolean shouldPropagate) {
		this.shouldPropagate = shouldPropagate;
	}

	public void setTransactionIds(List<int[]> transactionIds) {
		this.transactionIds = transactionIds;
	}

	private void clear() {
		this.vertexIds.clear();
		this.transactionIds.clear();
	}

	public void addFrequentPattern(int[] vertices, int[] transactions) {
		this.vertexIds.add(vertices);
		this.transactionIds.add(transactions);
	}

	public void setPropagationStatus(boolean ps) {
		this.shouldPropagate = ps;
	}

	public boolean shouldPropagate() {
		return this.shouldPropagate;
	}

	public int[] getTransactionIds() {
		return transactionIds.get(0);
	}

	@Override
	public void readFields(DataInput dataInput) throws IOException {
		this.clear();
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
			 * Setting the list of transaction id arrays into the item vertex
			 * value.
			 */
			JSONArray txnIdsArrayList = itemVertexValueJSONArray
					.getJSONArray(TRANSACTION_IDS_LIST_INDEX);
			for (int i = 0; i < txnIdsArrayList.length(); i++) {
				JSONArray txnIdsArray = txnIdsArrayList.getJSONArray(i);
				int[] txnIds = new int[txnIdsArray.length()];
				for (int j = 0; j < txnIdsArray.length(); j++) {
					txnIds[j] = txnIdsArray.getInt(j);
				}
				this.transactionIds.add(i, txnIds);
			}

			/**
			 * Setting the list of vertex id arrays into the item vertex value.
			 */
			JSONArray vertexIdsArrayList = itemVertexValueJSONArray
					.getJSONArray(VERTEX_IDS_LIST_INDEX);
			for (int i = 0; i < vertexIdsArrayList.length(); i++) {
				JSONArray vertexIdsArray = vertexIdsArrayList.getJSONArray(i);
				int[] txnIds = new int[vertexIdsArray.length()];
				for (int j = 0; j < vertexIdsArray.length(); j++) {
					txnIds[j] = vertexIdsArray.getInt(j);
				}
				this.vertexIds.add(i, txnIds);
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
			itemVertexValueJSONArray.put(TRANSACTION_IDS_LIST_INDEX,
					this.transactionIds);
			itemVertexValueJSONArray.put(VERTEX_IDS_LIST_INDEX, this.vertexIds);
		} catch (JSONException e) {
			e.printStackTrace();
		}

		String itemVertexValueJSONString = itemVertexValueJSONArray.toString();
		if (itemVertexValueJSONString.length() >= 16 * 1024) {
			byte[] byteArray = itemVertexValueJSONString.getBytes("utf-8");
			dataOut.writeShort(byteArray.length);
			dataOut.write(byteArray);
		} else {
			dataOut.writeUTF(itemVertexValueJSONString);
		}
	}

	@Override
	public String toString() {
		StringBuffer buf = new StringBuffer();
		JSONArray itemVertexValueJSONArray = new JSONArray();
		try {
			itemVertexValueJSONArray.put(SHOULD_PROPOGATE_INDEX,
					this.shouldPropagate);
			itemVertexValueJSONArray.put(TRANSACTION_IDS_LIST_INDEX,
					this.transactionIds);
			itemVertexValueJSONArray.put(VERTEX_IDS_LIST_INDEX, this.vertexIds);
		} catch (JSONException e) {
			e.printStackTrace();
		}
		buf.append(itemVertexValueJSONArray.toString());
		return buf.toString();
	}

}
