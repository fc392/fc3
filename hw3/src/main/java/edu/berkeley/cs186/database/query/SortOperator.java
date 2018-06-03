package edu.berkeley.cs186.database.query;

import edu.berkeley.cs186.database.Database;
import edu.berkeley.cs186.database.DatabaseException;
import edu.berkeley.cs186.database.databox.DataBox;
import edu.berkeley.cs186.database.table.Record;
import edu.berkeley.cs186.database.table.Schema;
import edu.berkeley.cs186.database.common.BacktrackingIterator;
import edu.berkeley.cs186.database.common.Pair;
import edu.berkeley.cs186.database.io.Page;

import java.util.*;


public class SortOperator  {
  private Database.Transaction transaction;
  private String tableName;
  private Comparator<Record> comparator;
  private Schema operatorSchema;
  private int numBuffers;

  public SortOperator(Database.Transaction transaction, String tableName, Comparator<Record> comparator) throws DatabaseException, QueryPlanException {
    this.transaction = transaction;
    this.tableName = tableName;
    this.comparator = comparator;
    this.operatorSchema = this.computeSchema();
    this.numBuffers = this.transaction.getNumMemoryPages();
  }

  public Schema computeSchema() throws QueryPlanException {
    try {
      return this.transaction.getFullyQualifiedSchema(this.tableName);
    } catch (DatabaseException de) {
      throw new QueryPlanException(de);
    }
  }


  public class Run {
    private String tempTableName;

    public Run() throws DatabaseException {
      this.tempTableName = SortOperator.this.transaction.createTempTable(SortOperator.this.operatorSchema);
    }

    public void addRecord(List<DataBox> values) throws DatabaseException {
      SortOperator.this.transaction.addRecord(this.tempTableName, values);
    }

    public void addRecords(List<Record> records) throws DatabaseException {
      for (Record r: records) {
        this.addRecord(r.getValues());
      }
    }

    public Iterator<Record> iterator() throws DatabaseException {
      return SortOperator.this.transaction.getRecordIterator(this.tempTableName);
    }

    public String tableName() {
      return this.tempTableName;
    }
  }


  /**
   * Returns a NEW run that is the sorted version of the input run.
   * Can do an in memory sort over all the records in this run
   * using one of Java's built-in sorting methods.
   * Note: Don't worry about modifying the original run.
   * Returning a new run would bring one extra page in memory beyond the
   * size of the buffer, but it is done this way for ease.
   */
  public Run sortRun(Run run) throws DatabaseException {
	  Run newRun = new Run();
	  List<Record> allRecords = new ArrayList<>();
	  Iterator<Record> tempIter = run.iterator();
	  while(tempIter.hasNext()){
		  allRecords.add(tempIter.next());
	  }
	  allRecords.sort(comparator);
	  newRun.addRecords(allRecords);
	  return newRun;
  }



  /**
   * Given a list of sorted runs, returns a new run that is the result
   * of merging the input runs. You should use a Priority Queue (java.util.PriorityQueue)
   * to determine which record should be should be added to the output run next.
   * It is recommended that your Priority Queue hold Pair<Record, Integer> objects
   * where a Pair (r, i) is the Record r with the smallest value you are
   * sorting on currently unmerged from run i.
   */
  public Run mergeSortedRuns(List<Run> runs) throws DatabaseException {
	  Run newRun = new Run();
	  Queue<Pair<Record, Integer>> priorityQueue = new PriorityQueue<>(new RecordPairComparator());
	  Vector<Iterator<Record>> iterVector = new Vector<>();
	  Iterator<Record> tempIterator;
	  Pair<Record,Integer> tempPair;
	  /* build priorityQueue */
	  int i = 0;
	  for (Run run:runs){
		  tempIterator = run.iterator();
		  iterVector.add(tempIterator);
		  if(tempIterator.hasNext()){
			  priorityQueue.add(new Pair<Record, Integer>(tempIterator.next(),i));
		  }
		  i++;
	  }
	  /* do the merge */
	  while(!priorityQueue.isEmpty()){
		  tempPair = priorityQueue.poll();
		  int index = tempPair.getSecond();
		  Record tempRecord = tempPair.getFirst();
		  if(iterVector.get(index).hasNext()){
			  priorityQueue.add(new Pair<Record, Integer>(iterVector.get(index).next(), index));
		  }
		  newRun.addRecord(tempRecord.getValues());
	  }
	  return newRun;
  }

  /**
   * Given a list of N sorted runs, returns a list of
   * sorted runs that is the result of merging (numBuffers - 1)
   * of the input runs at a time.
   */
  public List<Run> mergePass(List<Run> runs) throws DatabaseException {
	  int runsSize = runs.size();
	  List<Run> mergedRuns = new ArrayList<Run>();
	  int i = 0;
	  while(true){
		  int start = i * (numBuffers - 1);
		  int end = (i + 1) * (numBuffers- 1);
		  if(end >= runsSize){
			  end = runsSize ;
			  List<Run> subRuns = runs.subList(start, end);
			  mergedRuns.add(mergeSortedRuns(subRuns));
			  break;
		  }
		  List<Run> subRuns = runs.subList(start, end);
		  mergedRuns.add(mergeSortedRuns(subRuns));
		  i++;
	  }
	  return mergedRuns;

  }


  /**
   * Does an external merge sort on the table with name tableName
   * using numBuffers.
   * Returns the name of the table that backs the final run.
   */
  public String sort() throws DatabaseException {
	  BacktrackingIterator<Page> temp = this.transaction.getPageIterator(this.tableName);
	  temp.next();
	  List<Run> runList = new ArrayList<Run>();
	  /* split the table */
	  while(temp.hasNext()){
		  Run tempRun = createRun();
		  List<Record> recordList = new ArrayList<Record>();
		  Iterator<Record> recordIterator = this.transaction.getBlockIterator(this.tableName, temp, numBuffers - 1);
		  while(recordIterator.hasNext()){
			  recordList.add(recordIterator.next());
		  }
		  tempRun.addRecords(recordList);
		  runList.add(sortRun(tempRun));		  
	  }
	  /* do the merge */
	  while(runList.size() != 1){
		  runList = mergePass(runList);
	  }
	  return runList.get(0).tableName();

  }


  private class RecordPairComparator implements Comparator<Pair<Record, Integer>> {
    public int compare(Pair<Record, Integer> o1, Pair<Record, Integer> o2) {
      return SortOperator.this.comparator.compare(o1.getFirst(), o2.getFirst());

    }
  }

  public Run createRun() throws DatabaseException {
    return new Run();
  }



}
