package FDBProject.models;

import FDBProject.StatusCode;

import java.util.HashMap;
import java.util.Map;

import static FDBProject.StatusCode.PREDICATE_OR_EXPRESSION_INVALID;

public class AssignmentExpression {
  public enum Type {
    ONE_ATTR, // only one attribute is referenced, e.g. Salary = 1500, Name = "Bob"
    TWO_ATTRS, // two attributes are referenced, e.g. Salary = 1.5 * Age
  }

  private Type expressionType;

  public Type getExpressionType() {
    return expressionType;
  }

  private String leftHandSideAttrName; // e.g. Salary = 1.1 * Age
  private AttributeType leftHandSideAttrType;

  // either a specific value, or another attribute
  private Object rightHandSideValue = null; // in the example, it is 1.1
  private AlgebraicOperator rightHandSideOperator; // in the example, it is *
  private String rightHandSideAttrName; // in the example, it is Age
  private AttributeType rightHandSideAttrType;

  // e.g. Salary = 2000
  public AssignmentExpression(String leftHandSideAttrName, AttributeType leftHandSideAttrType, Object rightHandSideValue) {
    expressionType = Type.ONE_ATTR;
    this.leftHandSideAttrName = leftHandSideAttrName;
    this.leftHandSideAttrType = leftHandSideAttrType;
    this.rightHandSideValue = rightHandSideValue;
  }

  // e.g. Salary = Age * 2
  public AssignmentExpression(String leftHandSideAttrName, AttributeType leftHandSideAttrType, String rightHandSideAttrName, AttributeType rightHandSideAttrType, Object rightHandSideValue, AlgebraicOperator rightHandSideOperator) {
    expressionType = Type.TWO_ATTRS;
    this.leftHandSideAttrName = leftHandSideAttrName;
    this.leftHandSideAttrType = leftHandSideAttrType;
    this.rightHandSideAttrName = rightHandSideAttrName;
    this.rightHandSideAttrType = rightHandSideAttrType;
    this.rightHandSideValue = rightHandSideValue;
    this.rightHandSideOperator = rightHandSideOperator;
  }
  public StatusCode validate() {
    if (expressionType == Type.ONE_ATTR) {
      // e.g. Salary = 2000
      if (leftHandSideAttrType == AttributeType.NULL
          || (leftHandSideAttrType == AttributeType.INT && !(rightHandSideValue instanceof Integer) && !(rightHandSideValue instanceof Long))
          || (leftHandSideAttrType == AttributeType.DOUBLE && !(rightHandSideValue instanceof Double) && !(rightHandSideValue instanceof Float))
          || (leftHandSideAttrType == AttributeType.VARCHAR && !(rightHandSideValue instanceof String))) {
        return StatusCode.PREDICATE_OR_EXPRESSION_INVALID;
      }
    } else if (expressionType == Type.TWO_ATTRS) {
      // e.g. Salary = 10 * Age
      if (leftHandSideAttrType == AttributeType.NULL || rightHandSideAttrType == AttributeType.NULL
          || (leftHandSideAttrType == AttributeType.VARCHAR || rightHandSideAttrType == AttributeType.VARCHAR)
          || (leftHandSideAttrType != rightHandSideAttrType)
          || (leftHandSideAttrType == AttributeType.INT && !(rightHandSideValue instanceof Integer) && !(rightHandSideValue instanceof Long)
          || (leftHandSideAttrType == AttributeType.DOUBLE && !(rightHandSideValue instanceof Double) && !(rightHandSideValue instanceof Float)))) {
        return PREDICATE_OR_EXPRESSION_INVALID;
      }
    }
    return StatusCode.PREDICATE_OR_EXPRESSION_VALID;
  }

  public String getRightHandSideAttrName(){
    return rightHandSideAttrName;
  }

  public String getLeftHandSideAttrName(){
    return leftHandSideAttrName;
  }
  public Object getNewValue(Object objVal) {
    long val;
    long op;
//    if(expressionType != Type.ONE_ATTR){
//      System.out.println("Conversion must be a single attribute");
//      return null;
//    }
//    else{
//      System.out.println("This hit");
//    }
    if (objVal instanceof Integer) {
      val = ((Integer) objVal).longValue();
    } else if (objVal instanceof Double) {
      val = ((Double) objVal).longValue();
    } else if (objVal instanceof Float) {
      val = ((Float) objVal).longValue();
    } else if (objVal instanceof Long) {
      val = (Long) objVal;
    } else {
      System.out.println("LEFT FAILED");
      return -1;
    }
    if (rightHandSideValue instanceof Integer) {
      op = ((Integer) rightHandSideValue).longValue();
    } else if (rightHandSideValue instanceof Double) {
      op = ((Double) rightHandSideValue).longValue();
    } else if (rightHandSideValue instanceof Float) {
      op = ((Float) rightHandSideValue).longValue();
    } else if (rightHandSideValue instanceof Long) {
      op = (Long) rightHandSideValue;
    } else {
      System.out.println("OP FAILED");
      return -1;
    }

    if (rightHandSideOperator == AlgebraicOperator.MINUS) {
      val -= op;
    } else if (rightHandSideOperator == AlgebraicOperator.DIVISION) {
      val /= op;
    } else if (rightHandSideOperator == AlgebraicOperator.PLUS) {
      val += op;
    } else if (rightHandSideOperator == AlgebraicOperator.PRODUCT) {
      val *= op;
    } else {
      System.out.println("INVALID OP");
    }
    return val;
  }
}
