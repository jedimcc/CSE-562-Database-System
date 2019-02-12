package cse562_checkpoint3;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;

import net.sf.jsqlparser.expression.BinaryExpression;
import net.sf.jsqlparser.expression.BooleanValue;
import net.sf.jsqlparser.expression.DateValue;
import net.sf.jsqlparser.expression.DoubleValue;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.LongValue;
import net.sf.jsqlparser.expression.NullValue;
import net.sf.jsqlparser.expression.PrimitiveValue;
import net.sf.jsqlparser.expression.StringValue;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.PrimitiveType;

public class HashJoinOperator implements TupleIterator<Tuple>{

	TupleIterator<Tuple> tl;
	TupleIterator<Tuple> tr;
	Expression expression; // L.A = R.C only contains equal symbol
	boolean isOpen = true;
	
	ArrayList<Tuple> tupleListR = new ArrayList<Tuple>();// store all tuples of right table

	ArrayList<Tuple> tupleHashList = new ArrayList<Tuple>();
	int count = 0;// flag the position of tuple in the tupleHashList
	
	HashMap<Integer, ArrayList<Tuple>> hashcodeMap = new HashMap<>();
	
	LinkedHashMap<Column,PrimitiveValue> tempFullTupleMap1 = new LinkedHashMap<Column,PrimitiveValue>(); 
	Tuple tempTupleL = new Tuple(tempFullTupleMap1);
	//Integer hashcodeL = new Integer(0); 
	int hashcodeL = 0;
	
	LinkedHashMap<Column,PrimitiveValue> tempFullTupleMap = new LinkedHashMap<Column,PrimitiveValue>(); 
	Tuple tempTupleR = new Tuple(tempFullTupleMap);	
	
	LinkedHashMap<Column,PrimitiveValue> tempFullTupleMap3 = new LinkedHashMap<Column,PrimitiveValue>(); 
	Tuple temp = new Tuple(tempFullTupleMap3);
	
	
	public HashJoinOperator(TupleIterator<Tuple> tl, TupleIterator<Tuple> tr, Expression expression) {
	
		this.tl = tl;
        this.tr = tr;
        this.expression = expression;
        open();
			
	}
	
	
	@Override
	public void open() {
		if(!isOpen) {
			tl.open();
			tr.open();
			isOpen = true;
		}
		
	}

	@Override
	public void close() {
		 if(isOpen) {
			tl.close();
			//tr.close();
			isOpen = false;
		}	
		
	}

	@Override
	public Tuple getNext() {

		LinkedHashMap<Column,PrimitiveValue> tempFullTupleMap2 = new LinkedHashMap<Column,PrimitiveValue>(); 
		Tuple tupleCombine = new Tuple(tempFullTupleMap2);		
		
		ArrayList<Integer> hashList = new ArrayList<>();		
		
		//get columns
		ArrayList<Column> columnList =  expressionAnalyzer(expression);
		
	
		//write in right tuple into tuplelist
		//calculate hashcode for targeted column and build hashcodemap<hashcode, list<tuple>>
		if(tupleListR.isEmpty()) {
			while(tr.hasNext()) {				
				Tuple temp = tr.getNext();
				if(temp != null) {
					for(Column c: columnList) {
						if(temp.fullTupleMap.containsKey(c)) {
							int hashCode = hashCodeCalculator(temp.fullTupleMap.get(c));
							if(!hashList.contains(hashCode)) {
								hashList.add(hashCode);
							}
						}
					}
				
				
				    tupleListR.add(temp);
				}			
		    }
			
			for(int t: hashList) {
				ArrayList<Tuple> tupleList = new ArrayList<>();// store tuple according to hashcode
				for(Column c: columnList) {
					for(Tuple tu: tupleListR) {
						if(tu.fullTupleMap.containsKey(c)) {
							if(t == hashCodeCalculator(tu.fullTupleMap.get(c))) {
								tupleList.add(tu);
							}
						}
					}
				}
				hashcodeMap.put(t, tupleList);
			}			
			//tupleListR.removeAll(columnList);//how to save memory and delete tupleListR
		}	
		
		
		//inialize left tuple and get corresponding hashcode (first time)		
		if(tempTupleL.fullTupleMap.isEmpty()) {
			tempTupleL = tl.getNext();
			if(tempTupleL != null) {
				for(Column c: columnList) {
					if(tempTupleL.fullTupleMap.containsKey(c)) {
						 hashcodeL = hashCodeCalculator(tempTupleL.fullTupleMap.get(c));		
					}
			    }
			}
		}
		
		//HashJoin
		while(!hashcodeMap.containsKey(hashcodeL)) {
            tempTupleL = tl.getNext();
            if(tempTupleL == null) {
            	return null;
            }           
			for(Column c: columnList) {
				if(tempTupleL.fullTupleMap.containsKey(c)) {
					 hashcodeL = hashCodeCalculator(tempTupleL.fullTupleMap.get(c));		
				}
			}
		}
		
		if(hashcodeMap.containsKey(hashcodeL)) {
			tupleHashList = hashcodeMap.get(hashcodeL);
			if(!tupleHashList.isEmpty() && tupleHashList != null) {	
				//update tuple to be joined from the hashtuplelist
				if(count < tupleHashList.size()) {
					 temp = tupleHashList.get(count);					
				     count ++;
				}else {
					// count = 0;
					 temp = null;
				}			   												
				//join
				if(temp != null) {
					tupleCombine = joinTuple(tempTupleL,temp);
					return tupleCombine;
				}				

				if(temp == null) {
					
					//update tuple from left table
					tempTupleL = tl.getNext();
					if(tempTupleL == null) {
		            	return null;
		            }
					
					count = 0;
					
					for(Column c: columnList) {
						if(tempTupleL.fullTupleMap.containsKey(c)) {
							 hashcodeL = hashCodeCalculator(tempTupleL.fullTupleMap.get(c));		
						}
					}
					
					return this.getNext();
				}
			}
		}
		return null;
	}

	@Override
	public boolean hasNext() {
		
		if(tl.hasNext()) {
			return true;
		}else {
			tl.close();
		}
		
		if(tempTupleL == null) {
			return false;
		}
		
		//tl.hasNext() -> false
		if(count < tupleHashList.size()) {
			return true;
		}

		//this.close();
		return false;
	}

    public Tuple joinTuple(Tuple t1, Tuple t2){

		LinkedHashMap<Column,PrimitiveValue> outFullTupleMap = new LinkedHashMap<Column,PrimitiveValue>(); 
		Tuple outTuple = new Tuple(outFullTupleMap);
		outTuple.fullTupleMap.putAll(t1.fullTupleMap);
		outTuple.fullTupleMap.putAll(t2.fullTupleMap);
		
		return outTuple;		
	}

    public ArrayList<Column> expressionAnalyzer(Expression expression){
		
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
    
    public int hashCodeCalculator(PrimitiveValue t) {
		
    	PrimitiveType dataType = t.getType();
    	int temp = 0;
    	switch(dataType){
		case BOOL:		
		    temp = t.hashCode();
		    break;
		case DATE:
			temp = t.hashCode();
			break;
		case DOUBLE:
			temp = t.hashCode();
			break;
		case LONG:
			temp = t.hashCode();				 
			break;
		case STRING:
			temp = t.hashCode();
			break;
		case TIME:
			temp = t.hashCode();
			break;
		default:			
			break;
		}			      
	
    	return temp;
    	
    }

}
