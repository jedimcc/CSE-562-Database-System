package cse562_checkpoint3;

import java.util.LinkedHashMap;
import java.util.Set;

import net.sf.jsqlparser.expression.PrimitiveValue;
import net.sf.jsqlparser.expression.PrimitiveValue.InvalidPrimitive;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;

public class SubSelectOperator implements TupleIterator<Tuple>{

	
	TupleIterator<Tuple> to;
	String subSelectAlias;
	boolean isOpen = true;
	
	public SubSelectOperator(TupleIterator<Tuple> to,String subSelectAlias) {
		this.to = to;
		this.subSelectAlias = subSelectAlias;		
		open();
	}
	

	@Override
	public void open() {
		if(!isOpen) {
	    	   to.open();
	       }	
		
	}

	@Override
	public void close() {
		if(isOpen) {
			  to.close();
		}	
	}

	@Override
	public Tuple getNext() {
		
		if(to.hasNext()) {
					
			LinkedHashMap<Column,PrimitiveValue> fullTupleMap = new LinkedHashMap<Column,PrimitiveValue>(); 
			Tuple tuple = new Tuple(fullTupleMap);
			
			tuple = to.getNext();
			
			if(tuple == null) {
				return null;
			}
	
			if(subSelectAlias == null) {
				return tuple;
			}
			
			LinkedHashMap<Column,PrimitiveValue> fullTupleMaptemp = new LinkedHashMap<Column,PrimitiveValue>(); 
			Tuple tupletemp = new Tuple(fullTupleMaptemp);
			
			//Set<Column> columns = tuple.fullTupleMap.keySet();			
			for(Column c: tuple.fullTupleMap.keySet()) {
				
				Table t1 = new Table(subSelectAlias);
				//Column c1 = new Column(t1, c.getColumnName().toLowerCase());
				Column c1 = new Column(t1, c.getColumnName());
				//c.getTable().setName(subSelectAlias);		
				tupletemp.fullTupleMap.put(c1, tuple.fullTupleMap.get(c));
			}
					
			return tupletemp;
			
		}
			
		return null;
	}

	@Override
	public boolean hasNext() {
		if(to.hasNext()) {
			return true;
		}
		return false;
	}

}
