package FDBProject.models;

import FDBProject.StatusCode;
import FDBProject.utils.ComparisonUtils;

import static FDBProject.StatusCode.PREDICATE_OR_EXPRESSION_INVALID;

public class ComparisonPredicate {

  public enum Type {
    NONE, // meaning no predicate
    ONE_ATTR, // only one attribute is referenced, e.g. Salary < 1500, Name == "Bob"
    TWO_ATTRS, // two attributes are referenced, e.g. Salary >= 1.5 * Age
  }

  private Type predicateType = Type.NONE;

  public Type getPredicateType() {
    return predicateType;
  }

  private String leftHandSideAttrName; // e.g. Salary == 1.1 * Age
  private AttributeType leftHandSideAttrType;

  ComparisonOperator operator; // in the example, it is ==

  // either a specific value, or another attribute
  private Object rightHandSideValue = null; // in the example, it is 1.1
  private AlgebraicOperator rightHandSideOperator; // in the example, it is *

  private String rightHandSideAttrName; // in the example, it is Age
  private AttributeType rightHandSideAttrType;

  public String getLeftHandSideAttrName() {
    return leftHandSideAttrName;
  }

  public String getRightHandSideAttrName() {
    return rightHandSideAttrName;
  }

  public void setLeftHandSideAttrName(String leftHandSideAttrName) {
    this.leftHandSideAttrName = leftHandSideAttrName;
  }

  public AttributeType getLeftHandSideAttrType() {
    return leftHandSideAttrType;
  }

  public void setLeftHandSideAttrType(AttributeType leftHandSideAttrType) {
    this.leftHandSideAttrType = leftHandSideAttrType;
  }

  public ComparisonOperator getOperator() {
    return operator;
  }

  public void setOperator(ComparisonOperator operator) {
    this.operator = operator;
  }

  public Object getRightHandSideValue() {
    return rightHandSideValue;
  }

  public void setRightHandSideValue(Object rightHandSideValue) {
    this.rightHandSideValue = rightHandSideValue;
  }

  public ComparisonPredicate() {
    // None predicate by default
  }

  // e.g. Salary == 10000, Salary <= 5000
  public ComparisonPredicate(String leftHandSideAttrName, AttributeType leftHandSideAttrType, ComparisonOperator operator, Object rightHandSideValue) {
    predicateType = Type.ONE_ATTR;
    this.leftHandSideAttrName = leftHandSideAttrName;
    this.leftHandSideAttrType = leftHandSideAttrType;
    this.operator = operator;
    this.rightHandSideValue = rightHandSideValue;
  }

  // e.g. Salary == 1.1 * Age
  public ComparisonPredicate(String leftHandSideAttrName, AttributeType leftHandSideAttrType, ComparisonOperator operator, String rightHandSideAttrName, AttributeType rightHandSideAttrType, Object rightHandSideValue, AlgebraicOperator rightHandSideOperator) {
    predicateType = Type.TWO_ATTRS;
    this.leftHandSideAttrName = leftHandSideAttrName;
    this.leftHandSideAttrType = leftHandSideAttrType;
    this.operator = operator;
    this.rightHandSideAttrName = rightHandSideAttrName;
    this.rightHandSideAttrType = rightHandSideAttrType;
    this.rightHandSideValue = rightHandSideValue;
    this.rightHandSideOperator = rightHandSideOperator;
  }

  // validate the predicate, return PREDICATE_VALID if the predicate is valid
  public StatusCode validate() {
    if (predicateType == Type.NONE) {
      return StatusCode.PREDICATE_OR_EXPRESSION_VALID;
    } else if (predicateType == Type.ONE_ATTR) {
      // e.g. Salary > 2000
      if (leftHandSideAttrType == AttributeType.NULL
              || (leftHandSideAttrType == AttributeType.INT && !(rightHandSideValue instanceof Integer) && !(rightHandSideValue instanceof Long))
              || (leftHandSideAttrType == AttributeType.DOUBLE && !(rightHandSideValue instanceof Double) && !(rightHandSideValue instanceof Float))
              || (leftHandSideAttrType == AttributeType.VARCHAR && !(rightHandSideValue instanceof String))) {
        return StatusCode.PREDICATE_OR_EXPRESSION_INVALID;
      }
    } else if (predicateType == Type.TWO_ATTRS) {
      // e.g. Salary >= 10 * Age
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

  public boolean fitsCriteria(Object leftVal, Object rightVal) {
    long right;
    long left;
    long op;
    if (leftVal instanceof Integer) {
      left = ((Integer) leftVal).longValue();
    } else if (leftVal instanceof Double) {
      left = ((Double) leftVal).longValue();
    } else if (leftVal instanceof Float) {
      left = ((Float) leftVal).longValue();
    } else if (leftVal instanceof Long) {
      left = (Long) leftVal;
    } else {
      System.out.println("LEFT FAILED");
      return false;
    }
    if (rightVal instanceof Integer) {
      right = ((Integer) rightVal).longValue();
    } else if (rightVal instanceof Double) {
      right = ((Double) rightVal).longValue();
    } else if (rightVal instanceof Float) {
      right = ((Float) rightVal).longValue();
    } else if (rightVal instanceof Long) {
      right = (Long) rightVal;
    } else {
      System.out.println("RIGHT FAILED");
      return false;
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
      return false;
    }

    if (rightHandSideOperator == AlgebraicOperator.MINUS) {
      right -= op;
    } else if (rightHandSideOperator == AlgebraicOperator.DIVISION) {
      right /= op;
    } else if (rightHandSideOperator == AlgebraicOperator.PLUS) {
      right += op;
    } else if (rightHandSideOperator == AlgebraicOperator.PRODUCT) {
      right *= op;
    } else {
      System.out.println("INVALID OP");
    }

    return ComparisonUtils.compareTwoINT(left, right, operator);
  }
}
