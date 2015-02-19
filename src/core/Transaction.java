package core;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

/**
 * 
 * Class representing a transaction in the database.
 * 
 * @author Dhruv Sharma, Metacube Software Pvt. Ltd.
 *
 */
public class Transaction implements Writable {

	private int id;

	public Transaction(int id) {
		this.id = id;
	}

	public int getId() {
		return id;
	}

	public void setId(int id) {
		this.id = id;
	}

	@Override
	public String toString() {
		return Integer.toString(this.id);
	}

	@Override
	public void readFields(DataInput dataInput) throws IOException {
		this.id = dataInput.readInt();
	}

	@Override
	public void write(DataOutput dataOut) throws IOException {
		dataOut.writeInt(this.id);
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + id;
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
		Transaction other = (Transaction) obj;
		if (id != other.id)
			return false;
		return true;
	}

}
