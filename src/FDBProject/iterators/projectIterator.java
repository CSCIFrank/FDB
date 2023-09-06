package FDBProject.iterators;

import FDBProject.*;
import FDBProject.fdb.FDBHelper;
import FDBProject.fdb.FDBKVPair;
import FDBProject.models.Record;

import com.apple.foundationdb.Database;
import com.apple.foundationdb.KeyValue;
import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.directory.DirectorySubspace;
import com.apple.foundationdb.tuple.Tuple;

import java.util.ArrayList;
import java.util.List;

public class projectIterator extends Iterator {
    private java.util.Iterator<KeyValue> KVit;
    private boolean isDup;
    private List<String> setPath;
    private DirectorySubspace searchDir;
    private String targetAttr;
    private boolean isNew;

    private Cursor cursor;

    private Iterator it;
    public projectIterator(String tableName, String targetAttr, boolean isDup){
        //Generate record based on attr and iterate...right?
        this.tableName = tableName;
        this.targetAttr = targetAttr;
        this.isDup = isDup;
        this.isNew = true;

        db = FDBHelper.initialization();
        tx = FDBHelper.openTransaction(db);
        tblMeta = fetchMetadataByName(db, tx, tableName);
        cursor = new Cursor(Cursor.Mode.READ, tableName, tblMeta, tx);

        if(isDup){
            setPath = new ArrayList<>();
            setPath.add(tableName);
            setPath.add(DBConf.TABLE_PROJECT_STORE);
            setPath.add(targetAttr);
            searchDir = FDBHelper.createOrOpenSubspace(cursor.getTx(), setPath);
            Record r = cursor.getFirst();
            while(r != null){
                Tuple key = new Tuple().addObject(r.getValueForGivenAttrName(targetAttr));
                FDBKVPair pair = FDBHelper.getCertainKeyValuePairInSubdirectory(searchDir, tx, key, setPath);
                if(pair == null) {//unique
                    FDBKVPair newPair = new FDBKVPair(setPath, key, new Tuple());
                    FDBHelper.setFDBKVPair(searchDir, tx, newPair);
                }
                r = cursor.next(false);
            }
            KVit = FDBHelper.getKVPairIterableOfDirectory(searchDir, tx, false).iterator();
        }
    }
    //cloning using the same tx and db as its parent
    public projectIterator(String tableName, String targetAttr, boolean isDup, Transaction tx, Database db){
        //Generate record based on attr and iterate
        this.tableName = tableName;
        this.targetAttr = targetAttr;
        this.isDup = isDup;
        this.isNew = true;

        this.db = db;
        this.tx = tx;
        tblMeta = fetchMetadataByName(db, tx, tableName);
        cursor = new Cursor(Cursor.Mode.READ, tableName, tblMeta, tx);
    }

    public projectIterator(Iterator it, String attrName, boolean isDup){
        this.tableName = it.getTableName();
        this.targetAttr = attrName;
        this.isDup = isDup;
        this.isNew = false;

        this.it = it;
        tx = it.getTx();
        tblMeta = it.getMetaData();

        if(isDup){//if we need to remove duplicates
            setPath = new ArrayList<>();//create a temporary directory to store uniques
            setPath.add(tableName);
            setPath.add(DBConf.TABLE_PROJECT_STORE);
            setPath.add(targetAttr);
            searchDir = FDBHelper.createOrOpenSubspace(it.getTx(), setPath);
            Record r = it.next();
            while(r != null){
                Tuple key = new Tuple().addObject(r.getValueForGivenAttrName(targetAttr));
                FDBKVPair pair = FDBHelper.getCertainKeyValuePairInSubdirectory(searchDir, tx, key, setPath);
                if(pair == null) {//unique
                    FDBKVPair newPair = new FDBKVPair(setPath, key, new Tuple());
                    FDBHelper.setFDBKVPair(searchDir, tx, newPair);
                }
                r = it.next();
            }
            KVit = FDBHelper.getKVPairIterableOfDirectory(searchDir, tx, false).iterator();
        }
    }


    public Record next(){
        if(isDup){//if duplicates matter we look at our temp dir's iterator
            if(!KVit.hasNext()){
                return null;
            }
            Record r = new Record();
            KeyValue kv = KVit.next();
            r.setAttrNameAndValue(targetAttr, Tuple.fromBytes(kv.getKey()).get(1));
            return r;
        }
        else{//otherwise we use the standard cursor
            if(isNew){
                if(!cursor.isInitialized()){
                    return cursor.getFirst();
                }
                return cursor.next(false);
            }
            else{//iterators
                return it.next();
            }
        }
    }

    public void commit(){
        if(isDup){//if we create a temp subtable it will be removed after a commit
            FDBHelper.dropSubspace(tx, setPath);
        }
        if(isNew){//commit and close respective values
            cursor.commit();
            db.close();
        }
        else{
            it.commit();
        }
    }

    public void abort(){
        if(isDup){//same concept as commit
            FDBHelper.dropSubspace(tx, setPath);
        }
        if(isNew){
            cursor.abort();
            FDBHelper.abortTransaction(tx);
            db.close();
        }
        else{
            it.abort();
        }
    }

    public Iterator clone(Transaction tx, Database db){//generates a new iterator using the same tx and db
        if(isNew){
            return new projectIterator(tableName, targetAttr, isDup, tx, db);
        }
        return new projectIterator(it, targetAttr, isDup);//only need the tx for this one
    }
    public void update(String[] attrNames, Object[] attrValues){
        cursor.updateCurrentRecord(attrNames, attrValues);
    }

    public void delete(){
        cursor.deleteCurrentRecord();
    }
}
