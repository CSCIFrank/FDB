package FDBProject;

import FDBProject.fdb.FDBHelper;
import FDBProject.fdb.FDBKVPair;
import FDBProject.models.ComparisonPredicate;
import FDBProject.models.Record;
import FDBProject.models.TableMetadata;
import com.apple.foundationdb.Database;
import com.apple.foundationdb.Transaction;

import java.util.List;

public abstract class Iterator {

  public enum Mode {
    READ,
    READ_WRITE
  }

  private Mode mode;
  protected TableMetadata tblMeta;
  protected String tableName;
  protected Database db;
  protected Transaction tx;
  protected ComparisonPredicate pred;

  private boolean isClone = false;
  protected boolean stepFirst;

  public Transaction getTx(){
    return tx;
  }
  public Database getDB(){
    return db;
  }
  public String getTableName(){
    return tableName;
  }
  public TableMetadata getMetaData(){
    return tblMeta;
  }
  public Mode getMode() {
    return mode;
  };

  public void setMode(Mode mode) {
    this.mode = mode;
  };

  public abstract Record next();

  public abstract void commit();

  public abstract void abort();
  protected boolean isCloned(){
    return isClone;
  }
  public abstract Iterator clone(Transaction tx, Database db);
  public abstract void delete();

  public abstract void update(String[] attrNames, Object[] attrValues);
  protected TableMetadata fetchMetadataByName(Database db, Transaction tx, String tableName){
    TableMetadataTransformer tblTrans = new TableMetadataTransformer(tableName);
    List<FDBKVPair> kvPairs = FDBHelper.getAllKeyValuePairsOfSubdirectory(db, tx, tblTrans.getTableAttributeStorePath());
    return tblTrans.convertBackToTableMetadata(kvPairs);
  }
}
