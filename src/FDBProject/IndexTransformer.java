package FDBProject;

import FDBProject.fdb.FDBKVPair;
import FDBProject.models.IndexRecord;
import FDBProject.models.IndexType;
import FDBProject.models.NonClusteredBPTreeIndexRecord;
import FDBProject.models.NonClusteredHashIndexRecord;
import FDBProject.utils.IndexesUtils;
import com.apple.foundationdb.tuple.Tuple;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public class IndexTransformer {

    private List<String> indexStore;

    public IndexTransformer(String tableName, String attrName, IndexType indexType) {
        indexStore = new ArrayList<>();
        indexStore = IndexesUtils.getAttributeIndexDirPath(tableName, attrName);
        indexStore.add(indexType.name());
    }



    public List<String> getIndexStorePath() {
        return indexStore;
    }

    public FDBKVPair convertToIndexKVPair(IndexType idxType, Object attrVal, List<Object> pkValues) {
        FDBKVPair res = null;
        if (idxType == IndexType.NON_CLUSTERED_HASH_INDEX) {
            NonClusteredHashIndexRecord ncHashIndexRecord = new NonClusteredHashIndexRecord(attrVal, pkValues);
            res = new FDBKVPair(indexStore, ncHashIndexRecord.getKeyTuple(), ncHashIndexRecord.getValueTuple());
        } else if (idxType == IndexType.NON_CLUSTERED_B_PLUS_TREE_INDEX) {
            NonClusteredBPTreeIndexRecord ncBPTreeIndexRecord = new NonClusteredBPTreeIndexRecord(attrVal, pkValues);
            res = new FDBKVPair(indexStore, ncBPTreeIndexRecord.getKeyTuple(), ncBPTreeIndexRecord.getValueTuple());
        }
        return res;
    }

    public IndexRecord convertBackToIndexRecord(IndexType indexType, FDBKVPair kv) {
        if (kv == null) {
            return null;
        }
        Tuple keyTuple = kv.getKey();
        if (indexType == IndexType.NON_CLUSTERED_HASH_INDEX)
            return new NonClusteredHashIndexRecord(keyTuple);
        else if (indexType == IndexType.NON_CLUSTERED_B_PLUS_TREE_INDEX)
            return new NonClusteredBPTreeIndexRecord(keyTuple);
        return null;
    }
}