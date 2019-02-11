package cse562_checkpoint3;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Set;

import net.sf.jsqlparser.expression.BooleanValue;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.expression.PrimitiveValue;
import net.sf.jsqlparser.expression.PrimitiveValue.InvalidPrimitive;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.select.AllColumns;
import net.sf.jsqlparser.statement.select.AllTableColumns;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.SelectExpressionItem;
import net.sf.jsqlparser.statement.select.SelectItem;

public class ProjectOperator implements TupleIterator<Tuple>{
	
	TupleIterator<Tuple> ti;
	List<SelectItem> selectItems = new ArrayList<SelectItem>(); //SELECT A, B, C
	Evaluate evaluate;
	boolean isOpen = true;
	
	public ProjectOperator( TupleIterator<Tuple> ti, List<SelectItem> selectItems) {
		this.ti = ti;
		this.selectItems = selectItems;		
		this.open();
	}
	
	@Override
	public String toString(){
		return "Projection";
	}

	@Override
	public void open() {
       if(!isOpen) {
    	   ti.open();
       }		
	}

	@Override
	public void close() {
		if(isOpen) {
			ti.close();
		}		
	}

	@Override
	public Tuple getNext() {		

        LinkedHashMap<Column,PrimitiveValue> fullTupleMap = new  LinkedHashMap<Column,PrimitiveValue>(); 
		Tuple tuple = new Tuple(fullTupleMap);				
		tuple = ti.getNext();//get tuple from selectoperator
		
		//System.out.println("projection from selection "+ tuple.fullTupleMap.size() +" "+ tuple.fullTupleMap.isEmpty());
		if(tuple == null) {		
			this.close();
			return null;			
		}			
		
		LinkedHashMap<Column,PrimitiveValue> fullTupleMaptemp = new LinkedHashMap<Column,PrimitiveValue>(); 
		Tuple tempTuple = new Tuple(fullTupleMaptemp);		
		
		for(SelectItem s: selectItems) {
			
			//case1 alltablecolumns eg: C.*
			if(s instanceof AllTableColumns) {
				
				Table table = ((AllTableColumns) s).getTable();
				
				Set<Column> columns = tuple.fullTupleMap.keySet();
				for(Column c: columns) {
					Table temp = c.getTable();
					if(table.getName().equals(temp.getName())) {
						tempTuple.fullTupleMap.put(c, tuple.fullTupleMap.get(c));
					}
				}
			
			//Case2 AllColumns
			}else if(s instanceof AllColumns) {				
				return tuple;
			
			//Case3 expression
			}else if(s instanceof SelectExpressionItem) {
				Expression expression = ((SelectExpressionItem) s).getExpression();				
				
				//parser function
				//Case3.1 function : a) count() b) max() c) min() d) avg() e)distinct()?
				if(expression instanceof Function) {
					Function function = (Function) expression;
					String functionName = function.getName();
					if(functionName.equals("MAX")) {
						System.out.println("MAX");
					}else if(functionName.equals("MIN")) {
						System.out.println("MIN");
					}else if(functionName.equals("COUNT")) {
						
					}else if(functionName.equals("SUM")) {
						
					}else if (functionName.equals("AVG")) {
						
					}else {
						//???????????????????
					}
				}
				
				//Case3.2 expression
				String alias = null;
				if(((SelectExpressionItem) s).getAlias() != null) {
					 alias = ((SelectExpressionItem) s).getAlias();//projection alias name	
				}																
				
				evaluate = new Evaluate(tuple);				
				 try {
				    	if(expression == null) {
				    		 return tuple;
				    	}
				    	else {				    		
				    		 if(alias != null) {				    			 
				    			 
				    			 tempTuple.setValue(((Column) expression).getTable(),alias,(PrimitiveValue)(evaluate).eval(expression));
				    			// tempTuple.setValue(tuple.getTupleTable(),alias,(PrimitiveValue)(evaluate).eval(expression));
				    		 }else {
				    			 
				    			 tempTuple.setValue(((Column) expression).getTable(),((Column) (expression)).getColumnName(),(PrimitiveValue)(evaluate).eval(expression));
				    		  }
						     } 
					} catch (SQLException e) {
						e.printStackTrace();
						return null;
					}		
			}
				
		}	
		

		return tempTuple;
	}

	@Override
	public boolean hasNext() {
		
		if(ti.hasNext()) {
			return true;
		}
		
		close();
		return false;
	}
			

}

