package FDBProject.iterators;

import FDBProject.Iterator;
import FDBProject.models.AttributeType;
import FDBProject.models.ComparisonPredicate;
import FDBProject.models.Record;
import com.apple.foundationdb.Database;
import com.apple.foundationdb.Transaction;

import java.util.Map;
import java.util.Set;

public class joinIterator extends Iterator {
    private Iterator outerIt;//outer iterator
    private Iterator innerIt;//this is gonna need to be cloned

    private Iterator innerClone;
    private Record outerCurr;
    private Record innerCurr;
    private Set<String> attrSet;
    public joinIterator(Iterator outerIterator, Iterator innerIterator, ComparisonPredicate predicate, Set<String> attrNames){
        outerIt = outerIterator;
        innerIt = innerIterator;
        pred = predicate;
        attrSet = attrNames;

        innerClone = innerIterator.clone(innerIterator.getTx(), innerIterator.getDB());
        outerCurr = outerIt.next();
        innerCurr = innerClone.next();
    }
    public Record next(){
        while(outerCurr != null) {
            boolean valid = false;
            if(pred.getLeftHandSideAttrType() == AttributeType.VARCHAR){//string check "equality only"
                if(outerCurr.getValueForGivenAttrName(pred.getLeftHandSideAttrName()).equals(innerCurr.getValueForGivenAttrName(pred.getRightHandSideAttrName()))){
                    valid = true;
                }
            }
            else{//val check
                valid = pred.fitsCriteria(outerCurr.getValueForGivenAttrName(pred.getLeftHandSideAttrName()), innerCurr.getValueForGivenAttrName(pred.getRightHandSideAttrName()));
            }
            Record sol = new Record();
            if(valid){//insert based on insertion criteria
                Map<String, Record.Value> outerMap = outerCurr.getMapAttrNameToValue();
                Map<String, Record.Value> innerMap = innerCurr.getMapAttrNameToValue();
                if(attrSet == null){//insert based on specified naming conventions
                    for(Map.Entry<String, Record.Value> entry : outerMap.entrySet()){
                        if(innerMap.containsKey(entry.getKey())){
                            sol.setAttrNameAndValue(outerIt.getTableName() + "." + entry.getKey(), entry.getValue().getValue());
                        }
                        else{
                            sol.setAttrNameAndValue(entry.getKey(), entry.getValue().getValue());
                        }
                    }
                    for(Map.Entry<String, Record.Value> entry : innerMap.entrySet()){
                        if(outerMap.containsKey(entry.getKey())){
                            sol.setAttrNameAndValue(innerIt.getTableName() + "." + entry.getKey(), entry.getValue().getValue());
                        }
                        else{
                            sol.setAttrNameAndValue(entry.getKey(), entry.getValue().getValue());
                        }
                    }
                }
                else {
                    for (String i : attrSet) {
                        if(outerCurr.getValueForGivenAttrName(i) != null && innerCurr.getValueForGivenAttrName(i) != null){
                            sol.setAttrNameAndValue(outerIt.getTableName() + "." + i, outerCurr.getValueForGivenAttrName(i));
                            sol.setAttrNameAndValue(innerIt.getTableName() + "." + i, innerCurr.getValueForGivenAttrName(i));
                        }
                        else if(outerCurr.getValueForGivenAttrName(i) != null){
                            sol.setAttrNameAndValue(i, outerCurr.getValueForGivenAttrName(i));
                        }
                        else if(innerCurr.getValueForGivenAttrName(i) != null){
                            sol.setAttrNameAndValue(i, innerCurr.getValueForGivenAttrName(i));
                        }
                    }
                }
            }
            innerCurr = innerClone.next();
            if(innerCurr == null){//iterate through the next outer value
                outerCurr = outerIt.next();
                innerClone = innerIt.clone(innerIt.getTx(), innerIt.getDB());
                innerCurr = innerClone.next();
            }
            if(valid){//valid solution found
                return sol;
            }
        }
        return null;//reached the end
    }

    public void commit(){
        outerIt.commit();
        innerIt.commit();
    }

    public void abort(){
        outerIt.abort();
        innerIt.abort();
    }

    public Iterator clone(Transaction tx, Database db){//this is never used... :)
        Iterator outerClone = outerIt.clone(outerIt.getTx(), outerIt.getDB());
        Iterator innerClone = innerIt.clone(innerIt.getTx(), innerIt.getDB());
        return new joinIterator(outerClone, innerClone, pred, attrSet);
    }

    public void update(String[] attrNames, Object[] attrValues){//also never used but this should be the interaction
        innerClone.update(attrNames, attrValues);
        outerIt.update(attrNames, attrValues);
    }

    public void delete(){//same concept..never used but this should be the interaction
        innerClone.delete();
        outerIt.delete();
    }


}
