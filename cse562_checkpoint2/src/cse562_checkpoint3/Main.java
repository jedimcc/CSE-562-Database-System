package cse562_checkpoint3;

import java.util.*;
import java.io.*;
import java.sql.SQLException;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.commons.csv.CSVParser;

import java.io.Reader;
import java.io.InputStreamReader;
import net.sf.jsqlparser.*;
import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.expression.BinaryExpression;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.PrimitiveValue;
import net.sf.jsqlparser.expression.PrimitiveValue.InvalidPrimitive;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.expression.operators.conditional.OrExpression;
import net.sf.jsqlparser.parser.*;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.*;
import net.sf.jsqlparser.statement.create.table.*;
import net.sf.jsqlparser.statement.select.*;

public class Main {
   	
	static String prompt = "$> "; // expected prompt
	public static HashMap<String,CreateTable> tableMap = new HashMap<>();
	
	public static void main(String[] argsArray) throws Exception{					
		
		System.out.println(prompt);
		System.out.flush();
		
		Reader in = new InputStreamReader(System.in);		
		CCJSqlParser parser = new CCJSqlParser(in);
		Statement statement;

        while((statement = parser.Statement()) != null){     
        	
        	long startTime=System.currentTimeMillis(); //long endTime=System.
        	
		     if(statement instanceof CreateTable) {				        		
			      CreateTable ct = new CreateTable();
			      ct = (CreateTable)statement;			      
			      tableMap.put(ct.getTable().toString().toLowerCase(), ct);			      			    			      
			      
			 }else if(statement instanceof Select) {
				       SelectBody sb = ((Select) statement).getSelectBody();				        		
				       
				       TupleIterator<Tuple> t = treeParser(sb);
				       
				       while(t.hasNext()) {
		        			try {
		        				Tuple tuple = t.getNext();		        				
		        				if(tuple != null) {
		        					tuple.printTuple();
		        				}											
							} catch (Exception  e) {
								e.printStackTrace();
							}
	        			}
			 }          		
		     // read for next query
             System.out.println(prompt);
             System.out.flush();
             long endTime = System.currentTimeMillis(); 
             System.out.println("Time = " + (endTime -startTime)); 	
        }
	}
	
	public static TupleIterator<Tuple> treeParser(SelectBody sb) {
		
		TableOperator tol = null;
		SelectOperator so = null;
		SubSelectOperator sub = null;
		
		TupleIterator<Tuple> jo = null;
		ProjectOperator po = null;
		OrderByOperator oo = null;
		LimitOperator lo = null;
		GroupbyOperator go = null;
		
		TupleIterator<Tuple> trsel = null;
		TupleIterator<Tuple> tlsel = null;
		
		TupleIterator<Tuple> trpro = null;
		TupleIterator<Tuple> tlpro = null;
		
		
		if(sb instanceof PlainSelect) {			        			
			   PlainSelect plainSelect = (PlainSelect)sb;

			   //optimizing
			   /*
			   --------------------collecting information for optimizing----------------                                                                        
			                                                                           */
			   //get expression of selection for pushing down
			   Expression expsel = plainSelect.getWhere();
			   
			   //check or expression right now no projection push down for the case of or expression
			   boolean t = true;
			   
			   List<Join> joins = plainSelect.getJoins();
			   
			   //get expression of projection for pushing down
			   //consider selection projection group by
			   List<SelectItem> expProList = plainSelect.getSelectItems();	
			   
			   //get columns of groupby
			   List<Column> columnRefList = plainSelect.getGroupByColumnReferences();			   
			   /*
			   ----------------------optimization-------------------------------------                                                                     
	                                                                                 */
			   Optimizer op = new Optimizer();
			   
			   /*
               -------------------optimization for projection push down---------------
                                                                                     */
			   //optimize projection (projection push down)
			   //To be modified, when shall we choose projection push down			   
			   //Get projection selectItemList and set alias to null (R.A, C.*, S.C)
			   ArrayList<SelectItem> proList = op.setSelectItemAlias(expProList);
			   
			   //ProList -> empty means SELECT * FROM ... No need to push down
			   if(!proList.isEmpty() && t) {
				   //Get selection expressionList (R.A < S.C, R.B>3)
				   ArrayList<Expression> selList = op.expWhereAnalyzer(expsel);
				   //Split selection expression and add them into optSelList (R.A, S.C, R.B)		
				   ArrayList<Expression> optSelList = new ArrayList<>();
				   if(!selList.contains(null)) {
					   for(Expression exp: selList) {
					   optSelList.addAll(op.binarySplit(exp));
				       }
				   }
				   
				   //Add the column needed in selection into optProList for pushing down (R.A, C.*, S.C, R.B)
				   ArrayList<SelectItem> optProList = op.combine(proList, optSelList,columnRefList);
				   
				   //write data into HashMap<tableName, list<SelectItem>>(attribute of optimize)
				   op.optPro(optProList);
			   }
			   /* 
			   -----------------------optimization for selection push down----------------------
			                                                                                   */
			   //optimize join using selection push down
			   if(joins != null && expsel != null) {
				      // R.A> S.C AND R.A >3 AND R.B < 2 AND S.C =3;
					  ArrayList<Expression> expList = op.expWhereAnalyzer(expsel);// R.A>S.C, R.A>3, R.B<2, S.C=3
					  //ArrayList<Expression> expld = new ArrayList<>();
					  ArrayList<Expression> expLs = new ArrayList<>();
					  ArrayList<String> tableList1 = new ArrayList<>();	
					  
					  for(Expression exptemp : expList) {
						  ArrayList<String> array = op.binaryAnalyzer(exptemp);					  
						  if(array.size() == 1) { //fliter R.A > S.C
							  
							  //create tableList (R, S)
							  if(!tableList1.contains(array.get(0))) {
								  tableList1.add(array.get(0));
							  }
							  // add all single expressions to list for clustering
							  expLs.add(exptemp);
						      // op.selectelements.put(tableList1.get(0), exptemp);//Problems R.A>3 AND R.B<2 AND S.C=3
							   
						  } 
						  //find "equal expression"
						  else if(array.size() == 2) {							   
							   if(op.containsEqual(exptemp)) {
							        //String combstr = array.get(0) + array.get(1);
							        Combo combo = new Combo(array.get(0),array.get(1));
								   //expld.add(exptemp);
							        if(combo != null) {
							        	op.hashJoinMap.put(combo, exptemp);
							        }
							        
							   }
						  }
					  }
					  
					  //cluster expressions according to table name 
					  //combine expressions with same table name
					  //put combined expression and table name into hashmap
					  for(String temp : tableList1) {
						  ArrayList<Expression> tempList = new ArrayList<>();
						  for(Expression exp : expLs) {
							  if(temp.equals(op.binaryAnalyzer(exp).get(0))) {
								  tempList.add(exp);
							  }
						  }
						  Expression tempexp = op.combineExpression(tempList);
						  op.selectelements.put(temp, tempexp);
					  }

			   }    
			 
			  /*
			   -------------------------parser query-------------------------------------------
			                                                                                  */
			   FromItem fromItem = plainSelect.getFromItem();			   
			   if(fromItem instanceof SubSelect) {
				   SelectBody subBody = ((SubSelect) fromItem).getSelectBody();		
				   String subSelectAlias = ((SubSelect) fromItem).getAlias();
				   
				   //only consider plainSelect
				   sub = new SubSelectOperator(treeParser((PlainSelect)subBody),subSelectAlias);

			   }else if(fromItem instanceof Table) {
				   
				   Table tablel = ((Table) fromItem);
				   tol = new TableOperator(tablel);
				   
				   //record for table information in optimizer
				   if(tablel.getAlias()!= null) {
					   op.tableList.add(tablel.getAlias());
				   }else if(tablel.getName() != null) {
					   op.tableList.add(tablel.getName());
				   }
				   
				   //optimizing for left table				   
				   //projection push down				   
				   if(!proList.isEmpty() && op.optProMap.containsKey(tablel.getAlias())) {					   
					   tlpro = new ProjectOperator(tol,op.optProMap.get(tablel.getAlias()));					   
				   }
				   else if(!proList.isEmpty() && op.optProMap.containsKey(tablel.getName())) {
					   tlpro = new ProjectOperator(tol,op.optProMap.get(tablel.getName()));
				   }
				   else {					   
					   tlpro = tol;
				   }
				   
				   //selection push down
				   if((op.selectelements.containsKey(tablel.getAlias())) && op.selectelements != null) {						  
						  tlsel = new SelectOperator(tlpro, op.selectelements.get(tablel.getAlias()));  
				   }
				   else if((op.selectelements.containsKey(tablel.getName())) && op.selectelements != null) {
					      tlsel = new SelectOperator(tlpro, op.selectelements.get(tablel.getName()));
				   }
				   else {						  						 
					      tlsel = tlpro;
				   }    

			   }else if(fromItem instanceof SubJoin) {
				    System.out.println("subjoin");
			   }
			  
			  //parser Join
			  //SELECT * FROM R, S, T WHERE R.A = S.C, S.D = T.F;
			  //List<Join> joins = plainSelect.getJoins();
			  
			  if(joins != null) {
				  boolean isNatural = false;
				  boolean isInner = false;
				  boolean isSimple = false;
			      //count for join(eg: S, T)
				  int count = 0;

				  for(Join join : joins) {
					  count ++;	//count for join first time join with table
					  isSimple = join.isSimple();
					  isNatural = join.isInner();
					  isInner = join.isInner();
					  
					  FromItem fiJoin = join.getRightItem();
					  
					  if(fiJoin instanceof SubSelect) {
						  /*SelectBody subBody = ((SubSelect) fiJoin).getSelectBody();		
						  String subSelectAlias = ((SubSelect) fiJoin).getAlias();
						   
						  //only consider plainSelect
						  sub = new SubSelectOperator(treeParser((PlainSelect)subBody),subSelectAlias);*/
	
					  }else if(fiJoin instanceof SubJoin) {
						  //not consider for now
						  
					  }else if(fiJoin instanceof Table) {					  
						  
						  Table tabler = ((Table) fiJoin);
						  TableOperator tor = new TableOperator(tabler);
						  
						  //record for table information in optimizer
						  if(tabler.getAlias()!= null) {
							   op.tableList.add(tabler.getAlias());
						   }else if(tabler.getName() != null) {
							   op.tableList.add(tabler.getName());
						   }
						  
						  //optimizing for right table
						  //projection push down
						  if(!proList.isEmpty() && op.optProMap.containsKey(tabler.getAlias())) {							   
							   trpro = new ProjectOperator(tor,op.optProMap.get(tabler.getAlias()));						   
						   }
						  else if(!proList.isEmpty() && op.optProMap.containsKey(tabler.getName())) {
							   trpro = new ProjectOperator(tor,op.optProMap.get(tabler.getName()));
						  }
						  else {							   
							   trpro = tor;
						   }						  
						  
						  //selection push down
						  if((op.selectelements.containsKey(tabler.getAlias()))&& op.selectelements != null) {							  
							  trsel = new SelectOperator(trpro, op.selectelements.get(tabler.getAlias()));  
						  }
						  else if((op.selectelements.containsKey(tabler.getName()))&& op.selectelements != null) {
							  trsel = new SelectOperator(trpro, op.selectelements.get(tabler.getName())); 
						  }
						  else {							  
							  trsel = trpro;
						  }
						  
						    //Case 1 cross product
							if(isSimple) {
								Expression expression = plainSelect.getWhere();
							//	System.out.println("cross product " + isSimple);													
								
								//case 1.1 cross product
								if(expression == null) {
								//	System.out.println("cross product");
									
									if(count == 1){
										jo = new JoinOperator(tol, tor, expression);
									}else {
									    jo = new JoinOperator(jo, tor, expression);
									}
								}
															
								//case 1.2 cross product -> join
								if(expression != null) {
									if(count == 1){									
										if(sub == null) {											

											    //optimization join --> hash join
											    if(!op.hashJoinMap.isEmpty()) {
											    	Set<Combo> comboSet = op.hashJoinMap.keySet();
											    	for(Combo c: comboSet) {
											    		if(c.comboList.contains(tol.table.getAlias()) && c.comboList.contains(tor.table.getAlias())) {
											    			jo = new HashJoinOperator(tlsel,trsel,op.hashJoinMap.get(c));
											    			break;
											    		}
											    		
											    		if(c.comboList.contains(tol.table.getName()) && c.comboList.contains(tor.table.getName())) {
											    			jo = new HashJoinOperator(tlsel,trsel,op.hashJoinMap.get(c));
											    			break;
											    		}
											    	}
											    }
											    //no optimization for join 
											    else {
											    	jo = new JoinOperator(tlsel, trsel, expression);
											    }
					
										}else {	
												jo = new JoinOperator(sub, trsel, expression);											
										}										
									}else {	
										   
										//optimization join --> hash join
									    if(!op.hashJoinMap.isEmpty()) {
									    	Set<Combo> comboSet = op.hashJoinMap.keySet();
									    	for(Combo c: comboSet) {
									    		for(String str: op.tableList) {				    			
									    			if(c.comboList.contains(str) && c.comboList.contains(tor.table.getAlias())) {
										    			jo = new HashJoinOperator(jo,trsel,op.hashJoinMap.get(c));
										    			break;
										    		}
									    			
									    			if(c.comboList.contains(str) && c.comboList.contains(tor.table.getName())) {
									    				jo = new HashJoinOperator(jo,trsel,op.hashJoinMap.get(c));
									    				break;
									    			}
									    		}
									    	}
									    }
										//no optimizaiton for join
									    else {
									    	jo = new JoinOperator(jo, trsel, expression);
									    }	 									   
									}
								}							
							}
							//case 2 natural join
							else if(isNatural) {
							   System.out.println("isNatural " + isNatural);						   
							}
							
							//case 3 inner join
							else{
								System.out.println("inner ");
							}
	  
					  }
					  
				  }
              
              }
  
              //To be modified cuz of pushing down selection 
			  //parser selection(WHERE R.B = 0) SELECT R.A FROM R WHERE R.B = 0;
			  Expression expWhere = plainSelect.getWhere();
			  
			  //?????????
			  if (joins != null) {
				//  if(b) {
				//	  so =  new SelectOperator(jo, null);
				//  }else {
					  so = new SelectOperator(jo, expWhere);
				//  }
				  
			  }
			  //no join, but subselect
			  else if(sub != null){
				  so = new SelectOperator(sub, expWhere);
			  }
			  //no join no subselect
			  else {
				  so = new SelectOperator(tol, expWhere);
			  }
			  
              
			  //parser projection(SELECT R.A, R.B, R.C) SELECT R.A, R.B, R.C FROM R WHERE R.B = 0;
			  List<SelectItem> selectItems = plainSelect.getSelectItems();

			  po = new ProjectOperator(so, selectItems);
			  
			  //parser OrderBy(ORDERBY R.C, R.A DESC, R.B) SELECT R.A FROM R WHERE R.B = 0 ORDERBY R.C, R.A DESC, R.B;
			  List<OrderByElement> orderbyElements = plainSelect.getOrderByElements();
			  oo = new OrderByOperator(po, orderbyElements);
			  
			  //parser Limit(LIMIT 3) SELECT R.A FROM R WHERE R.B = 0 ORDERBY R.C, R.A DESC, R.B LIMIT 3;
			  Limit limit = plainSelect.getLimit();
			  lo = new LimitOperator(oo, limit);
			  
              //For checkpoint 3
			  //parser groupby 
			 // List<Column> columnRefList = plainSelect.getGroupByColumnReferences();
			  if(columnRefList != null) {				  
			  
			  Expression expHaving = plainSelect.getHaving();
			  
				   System.out.println("groupby");
			  			 
			  go = new GroupbyOperator(so,expHaving, columnRefList);
			  
			  }
			  
			  //parser Aggregate

			 // Distinct distinct = plainSelect.getDistinct();
			  			  	
		   }else if(sb instanceof Union) {
		            System.out.println("Union");					            		            
		   }
		   else {
		        System.out.println("can't handle it");
		   }
			
		return lo;
		
	}   
  
}


