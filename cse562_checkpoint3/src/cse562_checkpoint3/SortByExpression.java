package cse562_checkpoint3;

import java.sql.SQLException;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.schema.PrimitiveType;
import net.sf.jsqlparser.statement.select.OrderByElement;

public class SortByExpression implements Comparator<Tuple>{

	//Expression expression;
	List<OrderByElement> orderByElements;
	//boolean asc = true;
	
	public SortByExpression(List<OrderByElement> orderByElements) {
		this.orderByElements = orderByElements;	
	}	

	@Override
   public int compare(Tuple t1, Tuple t2) {

		int temp = 0;		
		int count = 0;
		
		int sizeOfElements = orderByElements.size();
		//System.out.println("size of orderbyElements " + sizeOfElements);
		
	    while(temp == 0 && count < sizeOfElements) {
	    	
			 temp = compareHelp(t1,t2,orderByElements.get(count));
			 count++;
			// System.out.println("count: "+count);
		}
		return temp;

		
	}
	
	public int compareHelp(Tuple t1, Tuple t2, OrderByElement orderByElement) {
		
		int help = 0;
		Evaluate evaluate1 = new Evaluate(t1);
		Evaluate evaluate2 = new Evaluate(t2);
		Expression expression = orderByElement.getExpression();
		boolean asc = orderByElement.isAsc();
		//System.out.println(" asc "+ asc);

		PrimitiveType dataType = null;
		try {
			dataType = (evaluate1).eval(expression).getType();
		} catch (SQLException e) {
			e.printStackTrace();
		}
		
		switch(dataType){
		case BOOL :
			 break;
		case DATE :
			 break;
		case DOUBLE :
			  try {
				help = (int)((evaluate1).eval(expression).toDouble()-(evaluate2).eval(expression).toDouble());
			} catch (SQLException e1) {
				e1.printStackTrace();
			}break;
		case LONG :			   
			 try {
				help = (int)((evaluate1).eval(expression).toLong()-((evaluate2).eval(expression).toLong()));
			   // System.out.println("tuple1: " + (evaluate1).eval(expression).toLong());
			   // System.out.println("tuple2: " + (evaluate2).eval(expression).toLong());
			 } catch (SQLException e) {
				e.printStackTrace();
			}break;
		case STRING :				 
			 try {
				help = (evaluate1).eval(expression).toString().compareTo((evaluate2).eval(expression).toString());
			} catch (SQLException e) {
				e.printStackTrace();
			}break;
		/*case TIME :
		    ;break;*/
		default:
		    break;
		}
			
		if(!asc) {
			help = Math.negateExact(help);
		}
		
		//System.out.println("help: "+help);
		return help;
	
		
	}
	
}
