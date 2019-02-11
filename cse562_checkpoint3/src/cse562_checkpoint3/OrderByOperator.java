package cse562_checkpoint3;

import java.util.*;

import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.PrimitiveValue;
import net.sf.jsqlparser.expression.PrimitiveValue.InvalidPrimitive;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.select.OrderByElement;
import net.sf.jsqlparser.statement.select.PlainSelect;

public class OrderByOperator implements TupleIterator<Tuple> {
		
	
	TupleIterator<Tuple> ti;
	Boolean isOpen = true;
	List<OrderByElement> orderByElements; 
	ArrayList<Tuple> tupleBuffer = new ArrayList<>();
	int count = 0;
	 	
	public OrderByOperator(TupleIterator<Tuple> ti, List<OrderByElement> orderByElements) {
		this.ti = ti;		
		this.orderByElements = orderByElements;
		//System.out.println(orderByElements == null);
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
		}		
		
	}

	@Override
	public Tuple getNext() {			
		
		LinkedHashMap<Column,PrimitiveValue> tempFullTupleMap = new LinkedHashMap<Column,PrimitiveValue>(); 
		Tuple tempTuple = new Tuple(tempFullTupleMap);		
		//if there is no orderby in query, return tuple
		//if(orderByElements.isEmpty()) { 
		// when there is no order by, plainSelect.getOrderByElements() return null
		if(orderByElements == null) {
			tempTuple =  ti.getNext();//get tuple from projectOperator
			if(tempTuple == null) {
				this.close();
				return null;
			}				
			return tempTuple;		
		}
		
		// if there is order by in query
		//expression and asc from orderByElement
		Expression expression;
		boolean asc = true;
		
		//add all tuples into tuple buffer
		// only do it once!!!!
		//ArrayList<Tuple> tupleBuffer = new ArrayList<>();
		// problems with condition how to realize only buffering once???? to be tested if has next works for it?
		if(tupleBuffer.isEmpty()) {
			
				while(ti.hasNext()) {			 	
				    tempTuple = ti.getNext();//get tuple from projectOperator				
					if(tempTuple == null) {
						this.close();
						break;
					}	
				
				    tupleBuffer.add(tempTuple);
				 //   System.out.println("tupleBuffer size: " + tupleBuffer.size());
			   }				
				
				
				/*//test print tuples in the tuple buffer
				System.out.println("check tupleBuffer --------");
				for(int i = 0; i < tupleBuffer.size();i++) {
					Tuple temp = tupleBuffer.get(i);
					try {
						temp.printTuple();
					} catch (InvalidPrimitive e) {
						e.printStackTrace();
					}
				}		
			    System.out.println("------------------------");	*/			
				
				Collections.sort(tupleBuffer, new SortByExpression(orderByElements));		
		}

	 /*	for(OrderByElement orderByElement : orderByElements) {			
			expression = orderByElement.getExpression();
			asc = orderByElement.isAsc();
            
			if(asc) {
				Collections.sort(tupleBuffer, new SortByExpression(expression));//ASCENDING
			}else {
				Collections.reverse(tupleBuffer);//DESCENDING
			}		    	
		}*/

	/*	//test print tuples in the tuple buffer
		System.out.println("check tupleBuffer after sort--------");
		for(int i = 0; i < tupleBuffer.size();i++) {
			Tuple temp = tupleBuffer.get(i);
			try {
				temp.printTuple();
			} catch (InvalidPrimitive e) {
				e.printStackTrace();
			}
		}		
	    System.out.println("-------------------------------------");*/
		
		
		/*// problem we can only output once and the tuple buffer is empty
		 * //if tupleBuffer is not empty, output only one tuple from tupleBuffer 
		if(!tupleBuffer.isEmpty()) {
			tempTuple = tupleBuffer.get(0); //return only one tuple
		    tupleBuffer.remove(0);//remove the one returned from tupleBuffer
		    return tempTuple;
		}
		
		if(tupleBuffer.isEmpty()) {
			this.close();
		}*/
		
		if(count < tupleBuffer.size()) {
			tempTuple = tupleBuffer.get(count);
			count++;
			//System.out.println(" count for output " + count);
			return tempTuple;
		}
		
		if(count >= tupleBuffer.size()){
			count = 0;
		}

		return null;
	}

	@Override
	public boolean hasNext() {
		
		//if(ti.hasNext() || !tupleBuffer.isEmpty()) {
		if(ti.hasNext() ||count < tupleBuffer.size()) {
			return true;
		}
				
		this.close();
		return false;
	}

}

