package simpledb;

import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;


/**
 * A {@code StringAggregator} computes some aggregate value over a set of {@code StringField}s.
 */
public class StringAggregator implements Aggregator {
	int gbfield;
	Type gbfieldtype;
	int afield;
	Op what;

	TupleDesc td;
	
	/**
	 * Constructs a {@code StringAggregator}.
	 * 
	 * @param gbfield
	 *            the 0-based index of the group-by field in the tuple, or {@code NO_GROUPING} if there is no grouping
	 * @param gbfieldtype
	 *            the type of the group by field (e.g., {@code Type.INT_TYPE}), or {@code null} if there is no grouping
	 * @param afield
	 *            the 0-based index of the aggregate field in the tuple
	 * @param what
	 *            aggregation operator to use -- only supports {@code COUNT}
	 * @throws IllegalArgumentException
	 *             if {@code what != COUNT}
	 */

	public StringAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
		// some code goes here
		this.gbfield = gbfield;
		this.gbfieldtype = gbfieldtype;
		this.afield = afield;
		this.what = what;
		if (gbfieldtype == null) {
			Type[] type = { Type.INT_TYPE };
			this.td = new TupleDesc(type);
		} else {
			Type[] type = { gbfieldtype, Type.INT_TYPE };
			this.td = new TupleDesc(type);
		}
	}

	/**
	 * Merges a new tuple into the aggregate, grouping as indicated in the constructor.
	 * 
	 * @param tup
	 *            the Tuple containing an aggregate field and a group-by field
	 */
	private Tuple createTuple(Tuple tup) {
		Tuple t = new Tuple(td);
		if (this.gbfieldtype == null) {
			t.setField(0, new IntField(1));
		} else {
			t.setField(0, tup.getField(gbfield));
			t.setField(1, new IntField(1));
		}
		return t;
	}

	
	


	
	public void merge(Tuple tup) {
		// some code goes here
		
	}

	/**
	 * Creates a {@code DbIterator} over group aggregate results.
	 *
	 * @return a {@code DbIterator} whose tuples are the pair ({@code groupVal}, {@code aggregateVal}) if using group,
	 *         or a single ({@code aggregateVal}) if no grouping. The aggregateVal is determined by the type of
	 *         aggregate specified in the constructor.
	 */
	public DbIterator iterator() {
		// some code goes here
		
		throw new UnsupportedOperationException("implement me");
	}
	
}
