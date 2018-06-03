package edu.berkeley.cs186.database.query;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

import edu.berkeley.cs186.database.Database;
import edu.berkeley.cs186.database.DatabaseException;
import edu.berkeley.cs186.database.common.BacktrackingIterator;
import edu.berkeley.cs186.database.databox.DataBox;
import edu.berkeley.cs186.database.io.Page;
import edu.berkeley.cs186.database.table.Record;
import edu.berkeley.cs186.database.table.Schema;

public class PNLJOperator extends JoinOperator {

  public PNLJOperator(QueryOperator leftSource,
                      QueryOperator rightSource,
                      String leftColumnName,
                      String rightColumnName,
                      Database.Transaction transaction) throws QueryPlanException, DatabaseException {
    super(leftSource,
          rightSource,
          leftColumnName,
          rightColumnName,
          transaction,
          JoinType.PNLJ);

  }

  public Iterator<Record> iterator() throws QueryPlanException, DatabaseException {
    return new PNLJIterator();
  }


  /**
   * PNLJ: Page Nested Loop Join
   *  See lecture slides.
   *
   * An implementation of Iterator that provides an iterator interface for this operator.
   *
   * Before proceeding, you should read and understand SNLJOperator.java
   *    You can find it in the same directory as this file.
   *
   * Word of advice: try to decompose the problem into distinguishable sub-problems.
   *    This means you'll probably want to add more methods than those given (Once again,
   *    SNLJOperator.java might prove to be a useful reference).
   */
  private class PNLJIterator extends JoinIterator {
    /**
     * Some member variables are provided for guidance, but there are many possible solutions.
     * You should implement the solution that's best for you, using any member variables you need.
     * You're free to use these member variables, but you're not obligated to.
     */

    private Iterator<Page> leftIterator = null;
    private Iterator<Page> rightIterator = null;
    private BacktrackingIterator<Record> leftRecordIterator = null;
    private BacktrackingIterator<Record> rightRecordIterator = null;
    private Record leftRecord = null;
    private Record nextRecord = null;
    private boolean newIterator = true;
   
    public PNLJIterator() throws QueryPlanException, DatabaseException {
      super();
      this.leftIterator = PNLJOperator.this.getPageIterator(getLeftTableName());
      this.rightIterator = PNLJOperator.this.getPageIterator(getRightTableName());
      if(this.leftIterator != null){
    	  this.leftIterator.next();
//    	  this.leftRecordIterator = PNLJOperator.this.getBlockIterator(getLeftTableName(), new Page[]{this.leftIterator.next()});
    	  this.leftRecordIterator = PNLJOperator.this.getBlockIterator(getLeftTableName(), leftIterator, 1);
    	  this.leftRecord = this.leftRecordIterator.hasNext()? this.leftRecordIterator.next():null;
      }
      if(this.rightIterator != null){
    	  this.rightIterator.next();
    	  this.rightRecordIterator = PNLJOperator.this.getBlockIterator(getRightTableName(), rightIterator, 1);
      }
      
      if(this.leftRecord != null){
    	  this.leftRecordIterator.mark();
      }
      try {
          fetchNextRecord();
        } catch (DatabaseException e) {
          this.nextRecord = null;
        }
    }
    
    private void nextLeftRecord() throws DatabaseException {
    	if(!leftRecordIterator.hasNext()){
    		if (!leftIterator.hasNext()) throw new DatabaseException("All Done!");
    		leftRecordIterator = PNLJOperator.this.getBlockIterator(getLeftTableName(), leftIterator, 1);
    		leftRecord = leftRecordIterator.next();
    		leftRecordIterator.mark();
    	}else{
    		leftRecord = leftRecordIterator.next();
    	}
    }
    
    private void nextrightRecordIterator() throws DatabaseException {
    	if(!rightIterator.hasNext()){
    		rightIterator = PNLJOperator.this.getPageIterator(getRightTableName());
    		rightIterator.next();
    	}
    	rightRecordIterator = PNLJOperator.this.getBlockIterator(getRightTableName(), rightIterator, 1);
		newIterator = true;	
    }
    
    private void checkNextPair(Record leftRecord, Record rightRecord){
    	DataBox leftJoinValue = this.leftRecord.getValues().get(PNLJOperator.this.getLeftColumnIndex());
        DataBox rightJoinValue = rightRecord.getValues().get(PNLJOperator.this.getRightColumnIndex());
        if (leftJoinValue.equals(rightJoinValue)) {
          List<DataBox> leftValues = new ArrayList<>(this.leftRecord.getValues());
          List<DataBox> rightValues = new ArrayList<>(rightRecord.getValues());
          leftValues.addAll(rightValues);
          this.nextRecord = new Record(leftValues);
        }
    }

    
	  /*
	   * right has Next?
	   * 	Y: right - > right.next;left not change
	   * 	N: left has Next?
	   * 		Y: left -> left->next; right reset
	   * 		N: rightIterator has Next?
	   * 			Y: rightIterator ->next right->next mark; left reset
	   * 			N: leftIterator has Next?
	   * 				Y: leftIterator->next left->next mark; rightIterator reset;
	   * 				N: finish!
	   */
    
    private void fetchNextRecord() throws DatabaseException {
        if (this.leftRecord == null) throw new DatabaseException("No new record to fetch");
        this.nextRecord = null;
        do{
        	if(this.rightRecordIterator.hasNext()){
        		Record rightRecord = this.rightRecordIterator.next();
        		if(newIterator){
        			this.rightRecordIterator.mark();
        			newIterator = false;
        		}        		
              	checkNextPair(leftRecord, rightRecord);
        	}else if(this.leftRecordIterator.hasNext()){
        		nextLeftRecord();
        		this.rightRecordIterator.reset();
        		newIterator = true;
        	}else if(this.rightIterator.hasNext()){
        		this.leftRecordIterator.reset();
        		nextLeftRecord();
        		nextrightRecordIterator();    
        	}else if(this.leftIterator.hasNext()){
        		nextLeftRecord();
        		nextrightRecordIterator();			
        	}else{
        		throw new DatabaseException("All done!");
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
    
  }
}
