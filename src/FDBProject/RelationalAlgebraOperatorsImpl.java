package FDBProject;

import FDBProject.fdb.FDBHelper;
import FDBProject.fdb.FDBKVPair;
import FDBProject.iterators.joinIterator;
import FDBProject.iterators.projectIterator;
import FDBProject.iterators.selectIterator;
import FDBProject.models.AssignmentExpression;
import FDBProject.models.ComparisonPredicate;
import FDBProject.models.Record;
import FDBProject.models.TableMetadata;
import com.apple.foundationdb.Database;
import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.directory.DirectorySubspace;
import com.apple.foundationdb.tuple.Tuple;

import java.util.*;

public class RelationalAlgebraOperatorsImpl implements RelationalAlgebraOperators {

  @Override
  public Iterator select(String tableName, ComparisonPredicate predicate, Iterator.Mode mode, boolean isUsingIndex) {
    if(predicate.validate() != StatusCode.PREDICATE_OR_EXPRESSION_VALID){//assume predicate is valid for one type
      return null;
    }
    selectIterator selectIt = new selectIterator(tableName, predicate, Iterator.Mode.READ, isUsingIndex);
    TableMetadata metaData = selectIt.getMetaData();
    //System.out.println("CMP: " + predicate.getLeftHandSideAttrType() + " " + metaData.getAttributes().get(predicate.getRightHandSideAttrName()));
    if(predicate.getPredicateType() == ComparisonPredicate.Type.TWO_ATTRS) {
      if (predicate.getLeftHandSideAttrType() != metaData.getAttributes().get(predicate.getRightHandSideAttrName())) {
        return null;
      }
    }
    return new selectIterator(tableName, predicate, mode, isUsingIndex);
  }

  @Override
  public Set<Record> simpleSelect(String tableName, ComparisonPredicate predicate, boolean isUsingIndex) {
    if(predicate.validate() != StatusCode.PREDICATE_OR_EXPRESSION_VALID){//assume predicate is valid for one type
      return null;
    }
    selectIterator selectIt = new selectIterator(tableName, predicate, Iterator.Mode.READ, isUsingIndex);
    TableMetadata metaData = selectIt.getMetaData();
    if(predicate.getPredicateType() == ComparisonPredicate.Type.TWO_ATTRS){
      if(predicate.getLeftHandSideAttrType() != metaData.getAttributes().get(predicate.getRightHandSideAttrName())){
        return null;
      }
    }
    else{
      System.out.println("CMP: " + predicate.getLeftHandSideAttrType() + " " + metaData.getAttributes().get(predicate.getRightHandSideAttrName()));
    }

    Set<Record> sol = new HashSet<>();
    Record r = selectIt.next();
    if(predicate.getPredicateType() == ComparisonPredicate.Type.ONE_ATTR){
      while(r != null){
        sol.add(r);
        r = selectIt.next();
      }
    }
    else{
      while(r != null){
        if(predicate.fitsCriteria(r.getValueForGivenAttrName(predicate.getLeftHandSideAttrName()), r.getValueForGivenAttrName(predicate.getRightHandSideAttrName()))){
          sol.add(r);
        }
        r = selectIt.next();
      }
    }
    return sol;
  }

  @Override
  public Iterator project(String tableName, String attrName, boolean isDuplicateFree) {
    projectIterator projectIt = new projectIterator(tableName, attrName, isDuplicateFree);
    if(!projectIt.getMetaData().doesAttributeExist(attrName)){
      return null;
    }
    return projectIt;
  }

  @Override
  public Iterator project(Iterator iterator, String attrName, boolean isDuplicateFree) {
    projectIterator projectIt = new projectIterator(iterator, attrName, isDuplicateFree);
    if(!projectIt.getMetaData().doesAttributeExist(attrName)){
      return null;
    }
    return projectIt;
  }

  @Override
  public List<Record> simpleProject(String tableName, String attrName, boolean isDuplicateFree) {
    List<Record> sol = new ArrayList<>();
    projectIterator projectIt = new projectIterator(tableName, attrName, isDuplicateFree);

    if(!projectIt.getMetaData().doesAttributeExist(attrName)){
      System.out.println("INVALID PROJECT");
      return null;
    }
    Record r = projectIt.next();
    while(r != null){
      sol.add(r);
      r = projectIt.next();
    }
    return sol;
  }

  @Override
  public List<Record> simpleProject(Iterator iterator, String attrName, boolean isDuplicateFree) {
    List<Record> sol = new ArrayList<>();
    projectIterator projectIt = new projectIterator(iterator, attrName, isDuplicateFree);

    if(!projectIt.getMetaData().doesAttributeExist(attrName)){
      System.out.println("INVALID PROJECT");
      return null;
    }
    Record r = projectIt.next();
    while(r != null){
      sol.add(r);
      r = projectIt.next();
    }
    return sol;
  }

  @Override
  public Iterator join(Iterator outerIterator, Iterator innerIterator, ComparisonPredicate predicate, Set<String> attrNames) {
    if(predicate.validate() != StatusCode.PREDICATE_OR_EXPRESSION_VALID){
      return null;
    }
    return new joinIterator(outerIterator, innerIterator, predicate, attrNames);
  }

  @Override
  public StatusCode insert(String tableName, Record record, String[] primaryKeys) {

    List<Object> primKeyVal = new ArrayList<>();
    List<String> attrKey = new ArrayList<>();
    List<Object> attrVal = new ArrayList<>();
    for(String i : primaryKeys){
      primKeyVal.add(record.getValueForGivenAttrName(i));
    }
    HashMap<String, Record.Value> recordMap = record.getMapAttrNameToValue();
    for(Map.Entry<String,Record.Value> entry : recordMap.entrySet()){
      attrKey.add(entry.getKey());
      attrVal.add(entry.getValue().getValue());
    }

    RecordsImpl recordImp = new RecordsImpl();
    return recordImp.insertRecord(tableName, primaryKeys, primKeyVal.toArray(), attrKey.toArray(new String[0]), attrVal.toArray());
  }

  @Override
  public StatusCode update(String tableName, AssignmentExpression assignExp, Iterator dataSourceIterator) {
    if(dataSourceIterator == null){//set all values by the assignExp
      Database db = FDBHelper.initialization();
      Transaction tx = FDBHelper.openTransaction(db);
      TableMetadataTransformer tblTrans = new TableMetadataTransformer(tableName);
      List<FDBKVPair> kvPairs = FDBHelper.getAllKeyValuePairsOfSubdirectory(db, tx, tblTrans.getTableAttributeStorePath());
      TableMetadata meta = tblTrans.convertBackToTableMetadata(kvPairs);
      Cursor cursor = new Cursor(Cursor.Mode.READ_WRITE, tableName, meta, tx);
      Record r = cursor.getFirst();
      while(r != null){
        Object newVal = assignExp.getNewValue(r.getValueForGivenAttrName(assignExp.getRightHandSideAttrName()));
        //System.out.println("NEW VAL NULL" + newVal.toString());
        r.setAttrNameAndValue(assignExp.getLeftHandSideAttrName(), newVal);
        HashMap<String, Record.Value> recData = r.getMapAttrNameToValue();

        List<String> attrKey = new ArrayList<>();
        List<Object> attrVal = new ArrayList<>();
        for(Map.Entry<String,Record.Value> entry : recData.entrySet()){
          attrKey.add(entry.getKey());
          attrVal.add(entry.getValue().getValue());
        }
        cursor.updateCurrentRecord(attrKey.toArray(new String[0]), attrVal.toArray());
        r = cursor.next(false);
      }
      cursor.commit();
      db.close();
    }
    else{//
      Record r = dataSourceIterator.next();
      while(r != null){
        Object newVal = assignExp.getNewValue(r.getValueForGivenAttrName(assignExp.getRightHandSideAttrName()));
        //System.out.println("NEW VAL" + newVal.toString());
        r.setAttrNameAndValue(assignExp.getLeftHandSideAttrName(), newVal);
        HashMap<String, Record.Value> recData = r.getMapAttrNameToValue();

        List<String> attrKey = new ArrayList<>();
        List<Object> attrVal = new ArrayList<>();
        for(Map.Entry<String,Record.Value> entry : recData.entrySet()){
          attrKey.add(entry.getKey());
          attrVal.add(entry.getValue().getValue());
        }
        dataSourceIterator.update(attrKey.toArray(new String[0]), attrVal.toArray());
        r = dataSourceIterator.next();
      }
      dataSourceIterator.commit();
    }
    return StatusCode.SUCCESS;
  }

  @Override
  public StatusCode delete(String tableName, Iterator iterator) {
    if(iterator == null){
      Database db = FDBHelper.initialization();
      Transaction tx = FDBHelper.openTransaction(db);
      TableMetadataTransformer tblTrans = new TableMetadataTransformer(tableName);
      FDBHelper.dropSubspace(tx, tblTrans.getTableAttributeStorePath());
      tx.commit();
      db.close();
    }
    else{
      Record r = iterator.next();
      while(r != null){
        iterator.delete();
        r = iterator.next();
      }
      iterator.commit();
    }
    return StatusCode.SUCCESS;
  }
}
