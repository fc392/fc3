package edu.berkeley.cs186.database.query;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

import edu.berkeley.cs186.database.Database;
import edu.berkeley.cs186.database.DatabaseException;
import edu.berkeley.cs186.database.databox.DataBox;
import edu.berkeley.cs186.database.io.Page;
import edu.berkeley.cs186.database.table.Record;
import edu.berkeley.cs186.database.table.RecordIterator;
import edu.berkeley.cs186.database.table.Schema;

public class SortMergeOperator extends JoinOperator {

  public SortMergeOperator(QueryOperator leftSource,
           QueryOperator rightSource,
           String leftColumnName,
           String rightColumnName,
           Database.Transaction transaction) throws QueryPlanException, DatabaseException {
    super(leftSource, rightSource, leftColumnName, rightColumnName, transaction, JoinType.SORTMERGE);

  }

  public Iterator<Record> iterator() throws QueryPlanException, DatabaseException {
    return new SortMergeOperator.SortMergeIterator();
  }

  /**
   * An implementation of Iterator that provides an iterator interface for this operator.
   *
   * Before proceeding, you should read and understand SNLJOperator.java
   *    You can find it in the same directory as this file.
   *
   * Word of advice: try to decompose the problem into distinguishable sub-problems.
   *    This means you'll probably want to add more methods than those given (Once again,
   *    SNLJOperator.java might be a useful reference).
   * 
   * Also, see discussion slides for week 7. 
   */
  private class SortMergeIterator extends JoinIterator {
    /** 
    * Some member variables are provided for guidance, but there are many possible solutions.
    * You should implement the solution that's best for you, using any member variables you need.
    * You're free to use these member variables, but you're not obligated to.
    */

     private String leftTableName;
     private String rightTableName;
     private RecordIterator leftIterator;
     private RecordIterator rightIterator;
     private Record leftRecord;
     private Record nextRecord;
     private Record rightRecord;
     private boolean marked;
     private boolean completed;
     private LR_RecordComparator LRcomparator;
     
    public SortMergeIterator() throws QueryPlanException, DatabaseException {
      super();
      this.leftTableName = new SortOperator(SortMergeOperator.this.getTransaction(),getLeftTableName(), new LeftRecordComparator()).sort();
      this.rightTableName = new SortOperator(SortMergeOperator.this.getTransaction(),getRightTableName(), new RightRecordComparator()).sort();
      this.leftIterator = SortMergeOperator.this.getRecordIterator(leftTableName);
      this.rightIterator = SortMergeOperator.this.getRecordIterator(rightTableName);
      this.leftRecord = this.leftIterator.hasNext()? this.leftIterator.next():null;
      this.rightRecord = this.rightIterator.hasNext()? this.rightIterator.next():null;
      this.marked = false;
      this.completed = false;
      this.LRcomparator = new LR_RecordComparator();
      try {
          fetchNextRecord();
        } catch (DatabaseException e) {
          this.nextRecord = null;
        }
    }
    
    private boolean nextRightRecord(){
    	if(this.rightIterator.hasNext()){
			this.rightRecord = this.rightIterator.next();
			return true;
		} else completed = true;
    	return false;
    }
    
    private boolean nextLeftRecord(){
    	this.marked = false;
    	if(this.leftIterator.hasNext()){
			this.leftRecord = this.leftIterator.next();
			return true;
		} else completed = true;
    	return false;
    }
    
    
    private void UpdateNextRecord(){
    	List<DataBox> leftValues = new ArrayList<>(this.leftRecord.getValues());
        List<DataBox> rightValues = new ArrayList<>(rightRecord.getValues());
        leftValues.addAll(rightValues);
        this.nextRecord = new Record(leftValues);
    }
    
    private void resetRightRecord(){
    	this.rightIterator.reset();
		this.rightRecord = this.rightIterator.next();
    }
    
    /* 
     * marked?
     * 	Y: L < R?
     * 		Y:R-reset, R-next, L-next, marked = false,
     * 		N:new nextRecord, R-next
     * 	N: 
     * 		L == R: R-marked, marked = true, new nextRecord, R-next
     * 		L < R : L-next = L->hasNext()? L->next:null 
     * 		L > R : R-next = R->hasNext()? R->next:null 
     * 
     */
    
    
    private void fetchNextRecord() throws DatabaseException {
        if (this.leftRecord == null || this.rightRecord == null) throw new DatabaseException("No new record to fetch");
        this.nextRecord = null;
        if(completed) throw new DatabaseException ("all Done!");
        do{
        	if(marked){
        		if (LRcomparator.compare(leftRecord, rightRecord) < 0){
        			resetRightRecord();
        			nextLeftRecord();
        		} else{
        			UpdateNextRecord();
        			if(!nextRightRecord()){
        				completed = false;
        				if(nextLeftRecord()) resetRightRecord();
        			}
        		}
        	} else if(LRcomparator.compare(leftRecord, rightRecord) == 0) {        		
        		UpdateNextRecord(); 	          	
        		if(rightIterator.hasNext()){
        			rightIterator.mark();
        			rightRecord = rightIterator.next();
        			marked = true;
        		} else nextLeftRecord();        		 		
        	} else if(LRcomparator.compare(leftRecord, rightRecord) < 0) {
        		nextLeftRecord();
        	} else if(LRcomparator.compare(leftRecord, rightRecord) > 0) {
        		nextRightRecord();
        	}
        }while(!hasNext());
      }

    /**
     * Checks if there are more record(s) to yield
     *
     * @return true if this iterator has another record to yield, otherwise false
     */
    public boolean hasNext() {
    	return this.nextRecord != null;
    }

    /**
     * Yields the next record of this iterator.
     *
     * @return the next Record
     * @throws NoSuchElementException if there are no more Records to yield
     */
    public Record next() {
    	if (!this.hasNext()) {
            throw new NoSuchElementException();
          }

          Record nextRecord = this.nextRecord;
          try {
            this.fetchNextRecord();
          } catch (DatabaseException e) {
            this.nextRecord = null;
          }
          return nextRecord;
    }

    public void remove() {
      throw new UnsupportedOperationException();
    }


    private class LeftRecordComparator implements Comparator<Record> {
      public int compare(Record o1, Record o2) {
        return o1.getValues().get(SortMergeOperator.this.getLeftColumnIndex()).compareTo(
                o2.getValues().get(SortMergeOperator.this.getLeftColumnIndex()));
      }
    }

    private class RightRecordComparator implements Comparator<Record> {
      public int compare(Record o1, Record o2) {
        return o1.getValues().get(SortMergeOperator.this.getRightColumnIndex()).compareTo(
                o2.getValues().get(SortMergeOperator.this.getRightColumnIndex()));
      }
    }

    /**
    * Left-Right Record comparator
    * o1 : leftRecord
    * o2: rightRecord
    */
    private class LR_RecordComparator implements Comparator<Record> {
      public int compare(Record o1, Record o2) {
        return o1.getValues().get(SortMergeOperator.this.getLeftColumnIndex()).compareTo(
                o2.getValues().get(SortMergeOperator.this.getRightColumnIndex()));
      }
    }
  }
}
