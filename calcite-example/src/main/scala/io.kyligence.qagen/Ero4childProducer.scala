package io.kyligence.qagen

import io.kyligence.qagen.Ero4childProducer.EroType
import org.apache.calcite.rex.{RexCall, _}
import org.apache.calcite.sql.SqlKind

import java.util
import scala.collection.mutable
import scala.jdk.CollectionConverters._


object Ero4childProducer {
  type EroType = mutable.HashMap[Int, Comparable[_]]

  /**
   * <p>Visits an array of expressions, returning the logical 'and' of their
   * results.
   *
   * <p>If any of them returns false, returns false immediately; if they all
   * return true, returns true.
   *
   * @see #visitArrayOr
   * @see RexShuttle#visitArray
   */
  //~ Instance fields --------------------------------------------------------
  //~ Constructors -----------------------------------------------------------
  //~ Methods ----------------------------------------------------------------
  def visitArrayAnd(visitor: RexVisitor[Boolean], exprs: util.List[RexNode]): Boolean = {

    exprs.forEach(expr => {
      val b: Boolean = expr.accept(visitor)
      if (!b) return false
    })
    true
  }

  /**
   * <p>Visits an array of expressions, returning the logical 'or' of their
   * results.
   *
   * <p>If any of them returns true, returns true immediately; if they all
   * return false, returns false.
   *
   * @see #visitArrayAnd
   * @see RexShuttle#visitArray
   */
  def visitArrayOr(visitor: RexVisitor[Boolean], exprs: util.List[RexNode]): Boolean = {
    exprs.forEach(expr => {
      val b: Boolean = expr.accept(visitor)
      if (b) return true
    })
    false
  }
}

class ExoCannotFulfill extends RuntimeException {}

class Ero4childProducer(exprs: java.util.List[RexNode]) extends RexVisitor[EroType] {
  //exo stands for Expected reX Output, which should be a Any in most cases
  val rex2exo: mutable.Map[RexNode, Comparable[_]] = mutable.HashMap.empty

  val ero4child: EroType = mutable.HashMap.empty

  //this func takes a RexCall who acts like a predicate on current rel
  //this RexCall's return type must be Boolean
  //this func produces ERO(part of) for current rel's child
  def go(rootRex: RexNode, exo4root: Comparable[_]): EroType = {

    //    val valid: Boolean = rootRex match {
    //      case call: RexCall => {
    //        call.`type` match {
    //          case basic: BasicSqlType if basic.getValueType.getSqlTypeName == SqlTypeName.BOOLEAN => true
    //          case _ => false
    //        }
    //      }
    //      case _ => false
    //    }
    //    if (!valid) throw new IllegalStateException("rootRex should be RexCall returning Boolean")

    rex2exo(rootRex) = exo4root
    rootRex.accept(this)
  }

  //if constant return the value wrapped in Option, otherwise return None
  private def tryGetConstant(subtree: RexNode): Option[Comparable[_]] = {
    val constantFinder = new ConstantFinder(exprs)
    if (subtree.accept(constantFinder)) {
      //TODO:
      Option.apply(1)
    } else {
      Option.empty
    }
  }

  private def getExoFor(rex: RexNode): Comparable[_] = {
    if (!rex2exo.contains(rex)) {
      throw new IllegalArgumentException(s"rex2exo does not contain key $rex")
    }

    rex2exo(rex)
  }

  override def visitInputRef(inputRef: RexInputRef): EroType = {
    ero4child(inputRef.getIndex) = getExoFor(inputRef)
    ero4child
  }

  override def visitCall(call: RexCall): EroType = {
    val exo = getExoFor(call)

    call.getKind match {
      case SqlKind.CASE => {
        val index2constantValues = mutable.Map.empty[Int, Comparable[_]]
        call.operands.asScala.zipWithIndex.foreach(operandAndIndex => {
          if (!RexUtil.isCasePredicate(call, operandAndIndex._2)) {
            val option = tryGetConstant(call.operands.get(operandAndIndex._2))
            option.foreach(constant => index2constantValues.put(operandAndIndex._2, constant))
          }
        })

        val potentialCaseValueIndexs = List.range(0, call.operands.size())
          .filter(!RexUtil.isCasePredicate(call, _))
          .filter(i => {
            var ret = true
            index2constantValues.get(i).foreach(constant => {
              if (exo != constant) ret = false
            })
            ret
          })

        //quick check
        if(potentialCaseValueIndexs.isEmpty){
          throw new ExoCannotFulfill
        }

        potentialCaseValueIndexs.foreach(potentialCaseValueIndex =>{

        })

      }
    }

    var r: EroType = null
    call.operands.forEach(operand => {
      r = operand.accept(this)
    })
    r
  }

  override def visitLiteral(literal: RexLiteral): EroType = null

  override def visitLocalRef(localRef: RexLocalRef): EroType = {
    throw new IllegalStateException("RexLocalRef should have been removed by ExpansionShuttle")
  }

  override def visitOver(over: RexOver): EroType = {
    throw new IllegalStateException("RexOver should have been removed by ProjectToWindowRule or CalcToWindowRule")
  }

  override def visitCorrelVariable(correlVariable: RexCorrelVariable): EroType = {
    throw new IllegalStateException("Subquery which cannot be decorrelled is unsupported")
  }

  override def visitDynamicParam(dynamicParam: RexDynamicParam): EroType = {
    throw new IllegalStateException("dynamic param is unsupported")
  }

  override def visitRangeRef(rangeRef: RexRangeRef): EroType = {
    throw new IllegalStateException("TODO: RexRangeRef support")
  }

  override def visitFieldAccess(fieldAccess: RexFieldAccess): EroType = {
    throw new IllegalStateException("Subquery which cannot be decorrelled is unsupported")
  }

  override def visitSubQuery(subQuery: RexSubQuery): EroType = {
    throw new IllegalStateException("Subquery which cannot be decorrelled is unsupported")
  }

  override def visitTableInputRef(ref: RexTableInputRef): EroType = {
    throw new IllegalStateException("RexTableInputRef is unsupported")
  }

  override def visitPatternFieldRef(fieldRef: RexPatternFieldRef): EroType = {
    throw new IllegalStateException("RexPatternFieldRef is unsupported")
  }
}