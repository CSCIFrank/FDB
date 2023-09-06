package FDBProject.iterators;

import FDBProject.Cursor;
import FDBProject.Iterator;
import FDBProject.fdb.FDBHelper;
import FDBProject.models.*;
import com.apple.foundationdb.Database;
import com.apple.foundationdb.Transaction;

public class selectIterator extends Iterator {
    private Cursor cursor;
    private boolean isUsingIndex;

    public selectIterator(String tableName, ComparisonPredicate predicate, Iterator.Mode mode, boolean isUsingIndex) {
        db = FDBHelper.initialization();
        tx = FDBHelper.openTransaction(db);
        tblMeta = fetchMetadataByName(db, tx, tableName);
        pred = predicate;
        this.tableName = tableName;
        setMode(mode);
        Cursor.Mode m;
        if (mode == Mode.READ) {
            m = Cursor.Mode.READ;
        } else {
            m = Cursor.Mode.READ_WRITE;
        }
        this.isUsingIndex = isUsingIndex;//may be needed for records
        cursor = new Cursor(m, tableName, tblMeta, tx);
        //if its just one predicate we can use cursor to cheese otherwise we need to check each val
        if (predicate.getPredicateType() == ComparisonPredicate.Type.ONE_ATTR){
            //select should only be read
            Record.Value rhsVal = new Record.Value();
            rhsVal.setValue(predicate.getRightHandSideValue());
            cursor.enablePredicate(predicate.getLeftHandSideAttrName(), rhsVal, predicate.getOperator());
        }
    }

    public selectIterator(String tableName, ComparisonPredicate predicate, Iterator.Mode mode, boolean isUsingIndex, Transaction tx, Database db) {
        this.db = db;
        this.tx = tx;
        tblMeta = fetchMetadataByName(db, tx, tableName);
        pred = predicate;
        this.tableName = tableName;
        setMode(mode);
        Cursor.Mode m;
        if (mode == Mode.READ) {
            m = Cursor.Mode.READ;
        } else {
            m = Cursor.Mode.READ_WRITE;
        }
        this.isUsingIndex = isUsingIndex;//may be needed for records
        cursor = new Cursor(m, tableName, tblMeta, tx);
        //if its just one predicate we can use cursor to cheese otherwise we need to check each val
        if (predicate.getPredicateType() == ComparisonPredicate.Type.ONE_ATTR){
            //select should only be read
            Record.Value rhsVal = new Record.Value();
            rhsVal.setValue(predicate.getRightHandSideValue());
            cursor.enablePredicate(predicate.getLeftHandSideAttrName(), rhsVal, predicate.getOperator());
        }
    }


    public Record next(){
        if(!cursor.isInitialized()){
            return cursor.getFirst();
        }
        return cursor.next(false);
    }

    public void commit(){
        cursor.commit();
        db.close();
    }

    public void abort(){
        cursor.abort();
        db.close();
    }

    public Iterator clone(Transaction tx, Database db){
        return new selectIterator(tableName, pred, getMode(), isUsingIndex, tx, db);
    }

    public void update(String[] attrNames, Object[] attrValues){
        cursor.updateCurrentRecord(attrNames, attrValues);
    }

    public void delete(){
        cursor.deleteCurrentRecord();
    }
}
