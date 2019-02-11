package cse562_checkpoint3;

import java.util.ArrayList;
import java.util.List;

import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.schema.Column;

public class GroupbyOperator implements TupleIterator<Tuple>{
    
	TupleIterator<Tuple> ti;
	Expression expHaving;
	List<Column> columnRefList = new ArrayList<Column>();
	
	public GroupbyOperator(TupleIterator<Tuple> ti, Expression expHaving, List<Column> columnRefList ) {
		this.ti = ti;
		this.expHaving = expHaving;
		this.columnRefList = columnRefList;
	}

	@Override
	public void open() {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void close() {
		// TODO Auto-generated method stub
		
	}

	@Override
	public Tuple getNext() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public boolean hasNext() {
		// TODO Auto-generated method stub
		return false;
	}

}
