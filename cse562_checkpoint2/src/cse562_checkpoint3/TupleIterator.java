package cse562_checkpoint3;

public interface TupleIterator<Tuple> {

	public void open();
	public void close();
	public Tuple getNext();
  public boolean hasNext();

}
