package cse562_checkpoint3;

import java.util.LinkedHashMap;

import net.sf.jsqlparser.expression.PrimitiveValue;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.select.Limit;
import net.sf.jsqlparser.statement.select.PlainSelect;

public class LimitOperator implements TupleIterator<Tuple> {

	
	TupleIterator<Tuple> ti;
	Boolean isOpen = true;
	Limit limit;
	Long offset = (long) 0;
	Long rowCount = (long) 1; // consideration of hasNext()
	long count = 0;
	long countOffset = 0;
	
	public LimitOperator (TupleIterator<Tuple> ti, Limit limit ) {
		this.ti = ti;		
		this.limit = limit;
		open();
	}
		
	@Override
	public void open() {
		if(!isOpen) {
			ti.open();
			isOpen = true;
		}
		
	}

	@Override
	public void close() {
		if(isOpen) {
			ti.close();
			isOpen = false;
			
			offset = (long) 0;
			rowCount = (long) 0;
			count = 0;
			countOffset = 0;
		}			
	}

	@Override
	public Tuple getNext() {

		LinkedHashMap<Column,PrimitiveValue> tempFullTupleMap = new LinkedHashMap<Column,PrimitiveValue>(); 
		Tuple tempTuple = new Tuple(tempFullTupleMap);
		
		// get tuple from orderByOperator
		tempTuple = ti.getNext(); 
		
		// there is no tuple passed from iterator
		if(tempTuple == null) {
			return null;
		}
		
		//case1 no limit
		if(limit == null) {			
			return tempTuple;
		}
		
		//case2 limit in query
		offset = limit.getOffset();//if no offset in limit return 0?
		//System.out.println("offset: " + offset);
		rowCount =  limit.getRowCount();
		//System.out.println("rowCount:  " + rowCount);
		if(count < rowCount ) {	
			
			// offset in limit
			while(countOffset < offset) {
				  countOffset++;
				//System.out.println("countOffset: " + countOffset);
				return this.getNext(); //recursively call getNext() until countOffset = offset
				                       // then return the tuple and output according to count condition
			}                             
		
			count ++;
			//System.out.println("count: " + count);
			return tempTuple;
		}
		
		return null;
	}

	@Override
	public boolean hasNext() {
		
		//if(ti.getNext() == null || rowCount == 0 || count > rowCount) {
		// when count >= rowCount, terminate
		if(!ti.hasNext() || count >= rowCount) {
		 
		 this.close();
		 return false;	
		}			
		
		return true;		
	}

}

