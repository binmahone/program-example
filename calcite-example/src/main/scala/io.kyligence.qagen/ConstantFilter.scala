package io.kyligence.qagen

import org.apache.calcite.rex._

import java.util
import java.util.List

class ConstantFinder(exprs: List[RexNode]) extends RexVisitor[Boolean] {
  override def visitLiteral(literal: RexLiteral): Boolean = {
    return true
  }

  override def visitInputRef(inputRef: RexInputRef): Boolean = {
    return false
  }

  override def visitLocalRef(localRef: RexLocalRef): Boolean = {
    val expr = exprs.get(localRef.getIndex)
    return expr.accept(this)
  }

  override def visitOver(over: RexOver): Boolean = {
    return false
  }

  override def visitSubQuery(subQuery: RexSubQuery): Boolean = {
    return false
  }

  override def visitTableInputRef(ref: RexTableInputRef): Boolean = {
    return false
  }

  override def visitPatternFieldRef(fieldRef: RexPatternFieldRef): Boolean = {
    return false
  }

  override def visitCorrelVariable(correlVariable: RexCorrelVariable): Boolean = { // Correlating variables change when there is an internal restart.
    // Correlating variables are constant WITHIN A RESTART, so that's
    // good enough.
    return true
  }

  override def visitDynamicParam(dynamicParam: RexDynamicParam): Boolean = { // Dynamic parameters are constant WITHIN AN EXECUTION, so that's
    // good enough.
    return true
  }

  override def visitCall(call: RexCall): Boolean = { // Constant if operator is deterministic and all operands are
    // constant.
    return call.getOperator.isDeterministic && visitArrayAnd(call.getOperands)
  }

  private def visitArrayAnd(exprs: util.List[RexNode]): Boolean = {
    exprs.forEach(expr => {
      val b = expr.accept(this)
      if (!b) return false
    })
    true
  }

  override def visitRangeRef(rangeRef: RexRangeRef): Boolean = {
    return false
  }

  override def visitFieldAccess(fieldAccess: RexFieldAccess): Boolean = { // "<expr>.FIELD" is constant iff "<expr>" is constant.
    return fieldAccess.getReferenceExpr.accept(this)
  }
}