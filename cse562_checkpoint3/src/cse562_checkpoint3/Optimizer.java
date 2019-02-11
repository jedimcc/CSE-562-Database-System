package cse562_checkpoint3;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;

import net.sf.jsqlparser.expression.BinaryExpression;
import net.sf.jsqlparser.expression.BooleanValue;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.expression.LongValue;
import net.sf.jsqlparser.expression.PrimitiveValue;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.expression.operators.relational.EqualsTo;
import net.sf.jsqlparser.expression.operators.relational.ExpressionList;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.select.AllColumns;
import net.sf.jsqlparser.statement.select.AllTableColumns;
import net.sf.jsqlparser.statement.select.SelectExpressionItem;
import net.sf.jsqlparser.statement.select.SelectItem;

public class Optimizer {
	
	HashMap<String , Expression> selectelements = new HashMap<>();    
	
	Expression adex = null;
	
	//attribute for projection pushing down
	//Key -> table name, Value-> List of selectItem for specific table projection
	HashMap<String, ArrayList<SelectItem>> optProMap = new HashMap<>();

	HashMap<Combo, Expression> hashJoinMap = new HashMap<>();
	
	ArrayList<String> tableList = new ArrayList<>();
	
    public ArrayList<Expression> expWhereAnalyzer(Expression expression){
		
	    ArrayList<Expression> expList = new ArrayList<>();
	    //single expression
	    if(!( expression instanceof AndExpression)){
	    	expList.add(expression);
	    }
	    
	    while(expression instanceof AndExpression) {
			Expression l = ((AndExpression) expression).getLeftExpression();
			Expression r = ((AndExpression) expression).getRightExpression();
			expList.add(r);
			
			if(l instanceof AndExpression) {
				expression = l;
			}else {
				expList.add(l);
				expression = l;
			}
		}
	    
		return expList;		
	}
	
	
	public ArrayList<Column> binarySplit(Expression expression){
		
		ArrayList<Column> columnList = new ArrayList<Column>();
		if(expression instanceof BinaryExpression) {
			Expression l = ((BinaryExpression) expression).getLeftExpression();
			Expression r = ((BinaryExpression) expression).getRightExpression();
			
			if(l instanceof Column) {
				Column columnl = (Column)l;
				columnList.add(columnl);
			}
			
			if(r instanceof Column) {
				Column columnr = (Column)r;
				columnList.add(columnr);
			}		    
			return columnList;
		}				
		return null;		
	}
	
    public ArrayList<String> binaryAnalyzer(Expression expression){
		
		ArrayList<String> tableList = new ArrayList<>();
		if(expression instanceof BinaryExpression) {
			Expression l = ((BinaryExpression) expression).getLeftExpression();
			Expression r = ((BinaryExpression) expression).getRightExpression();
			
			if(l instanceof Column) {
				Column columnl = (Column)l;
				tableList.add(columnl.getTable().getName());
			}
			
			if(r instanceof Column) {
				Column columnr = (Column)r;
				tableList.add(columnr.getTable().getName());
			}		    
			return tableList;
		}				
		return null;
		
	}	
    
    public boolean containsEqual(Expression exp) {   	
    	if(exp instanceof EqualsTo) {
    		return true;
    	}    	
		return false;    	
    }
    
    public Expression combineExpression(ArrayList<Expression> expressionList) {		
    	if(!expressionList.isEmpty()) {   		  
    		 // BooleanValue t = BooleanValue.TRUE;
			  Expression temp = expressionList.get(0);
			  if(expressionList.size() == 1) {
				  return temp;
			  }else {
				  for(int i = 1; i < expressionList.size(); i++) {
					  Expression extemp = expressionList.get(i);
					  temp = new AndExpression(temp, extemp);
				  }
				  return temp;				  
			  }
        }  	
    	return null;    	
    }
 
    public ArrayList<SelectItem> setSelectItemAlias(List<SelectItem> expProList){   	    
    	ArrayList<SelectItem> temp = new ArrayList<>();
    	for(SelectItem s: expProList) {  
    		
    		if(s instanceof AllTableColumns) {
    			temp.add(s);
    		}    		 		
    		
    		if(s instanceof SelectExpressionItem) {    	
    			SelectExpressionItem st = new SelectExpressionItem();
				Expression exp = ((SelectExpressionItem) s).getExpression();
				
				//case of function eg: Count(R.A) AS Q
				if(exp instanceof Function) {
					Function function = (Function) exp;
	    			ExpressionList eList = function.getParameters();
	    			List<Expression> expList =  eList.getExpressions();
	    			
	    			if(expList != null) {
	    				for(Expression ex : expList) {	    					
	    					st.setExpression(ex);
	    					temp.add(st);
	    				}
	    			}	    			
				}
				//case of nonfunction expression eg: R.A
				else {
					st.setExpression(exp);   		
				    temp.add(st);
				}
				
    	    }    		
    	}   	
    	
    		return temp;
    	  	
    }
    
    //combine selectItem list of projection and selelction
    public ArrayList<SelectItem> combine(ArrayList<SelectItem> proList, ArrayList<Expression> optSelList, List<Column> columnRefList){
    	
    	//ArrayList<Expression> temp = new ArrayList<>();
    	for(Expression exp: optSelList) {
    		SelectExpressionItem tempItem = new SelectExpressionItem();
    		tempItem.setExpression(exp);
    		proList.add(tempItem);	
    	}
    	
    	if(columnRefList != null) {
    		for(Column c: columnRefList) {
    		SelectExpressionItem tempItem = new SelectExpressionItem();
    		tempItem.setExpression(c);
    		proList.add(tempItem);
    	    }
    	}  	
    		return proList;
    	    	
    }
    
    public void optPro(ArrayList<SelectItem> proList) {   	
    	//namelist storing tablename of alltablecolumns T.* -> T for filtering T.C
    	ArrayList<String> nameList = new ArrayList<>();
    	ArrayList<Column> columnList = new ArrayList<>();
    	
    	for(SelectItem temp : proList) {
    		
    		//put alltablecolumns into hashmap
    		if(temp instanceof AllTableColumns) {
    			ArrayList<SelectItem> tempList = new ArrayList<>();
    			Table table = ((AllTableColumns) temp).getTable();
    			String tableName = table.getName();
    			tempList.add(temp);
    			optProMap.put(tableName, tempList);
    			nameList.add(tableName);
    			
    		}else if(temp instanceof SelectExpressionItem) {
    			Expression expression = ((SelectExpressionItem) temp).getExpression();
    			Column column = (Column) expression;
    			String talbeName = column.getTable().getName();
    			
    			//if there is alltablecolumns,delete corresponding other columns T.*, T.C
    			//add all columns to columnlist
    			if(!nameList.contains(talbeName)) {
    				columnList.add(column);
    			}		
    		}
    	}
    	
    	//remove duplicated columns 
        //newColumnList (no duplicated columns)
    	ArrayList<Column> newColumnList = new ArrayList<>();
    	Iterator<Column> it = columnList.iterator();
    	while(it.hasNext()){          
    	        Column c = it.next();       
    	        if(!newColumnList.contains(c)){      
    	        	newColumnList.add(c);       
    	        }
    	}
    	
    	//cluster columns into groups according table name
    	ArrayList<String> tableNameList = new ArrayList<>();
    	
    	//save the table names
    	for(Column c : newColumnList) {   	    
    		if(!tableNameList.contains(c.getTable().getName())) {
    			tableNameList.add(c.getTable().getName());
    		}
    		
    	}
    	
    	// 1. put columns into arraylist according to table name
    	// 2. put tablename and corresponding column arraylist into hashmap
    	
    	/*//bug case there is only one table, column without table name
    	//eg: Select A, B from R WHERE B>3;
    	if(tableNameList.contains(null)) {
    		ArrayList<SelectItem> selectItemList = new ArrayList<>();
    		for(Column c : newColumnList) {
    			SelectExpressionItem st = new SelectExpressionItem();
				st.setExpression(c);
				selectItemList.add(st);
    		}
    		
    		//optProMap.put(s, selectItemList);
    		
    	}else {*/
    	    if(!tableNameList.contains(null)) {
    	        for(String s: tableNameList) {    		
    		        ArrayList<SelectItem> selectItemList = new ArrayList<>();
	    		    for(Column c : newColumnList) {  		
		    			if(s.equals(c.getTable().getName())) {
		    				
		    				SelectExpressionItem st = new SelectExpressionItem();
		    				st.setExpression(c);
		    				selectItemList.add(st);
		    			}
	    			
	    		    }
    	    
    	            optProMap.put(s, selectItemList);
    	        }		
    	    }
    	    
    	//}
    	
    	
    	   	
    }
    
}

