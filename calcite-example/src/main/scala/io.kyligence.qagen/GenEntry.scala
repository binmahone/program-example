package io.kyligence.qagen

import com.matt.test.calcite.CalciteUtils
import com.matt.test.calcite.sql.SqlHepTest
import org.apache.calcite.config.CalciteConnectionConfigImpl
import org.apache.calcite.jdbc.CalciteSchema
import org.apache.calcite.plan.hep.{HepPlanner, HepProgramBuilder}
import org.apache.calcite.plan.{ConventionTraitDef, RelOptCluster, RelOptUtil, RelTraitSet}
import org.apache.calcite.prepare.CalciteCatalogReader
import org.apache.calcite.rel.`type`.{RelDataType, RelDataTypeSystem}
import org.apache.calcite.rel.rules.{FilterJoinRule, PruneEmptyRules, ReduceExpressionsRule, SubQueryRemoveRule}
import org.apache.calcite.rel.{AbstractRelNode, RelDistributionTraitDef, RelNode, RelVisitor}
import org.apache.calcite.rex.{RexShuttle, RexVisitor, RexVisitorImpl}
import org.apache.calcite.sql.`type`.{SqlTypeFactoryImpl, SqlTypeName}
import org.apache.calcite.sql.fun.SqlStdOperatorTable
import org.apache.calcite.sql.parser.SqlParser
import org.apache.calcite.sql.validate.SqlValidatorUtil
import org.apache.calcite.sql2rel.{RelDecorrelator, SqlToRelConverter}
import org.apache.calcite.tools.Frameworks
import org.apache.calcite.util.{ReflectUtil, ReflectiveVisitor}
import org.slf4j.LoggerFactory

import java.util
import java.util.Properties
import scala.collection.StrictOptimizedIterableOps
import scala.io.Source
import scala.util.{Failure, Random, Success, Try}

object GenEntry extends App {
  val LOGGER = LoggerFactory.getLogger(classOf[SqlHepTest])

  //test scala
  def readTextFile(filename: String): Try[List[String]] = {
    Try(Source.fromFile(filename).getLines.toList)
  }

  val filename = "etc/passwdddd"
  readTextFile(filename) match {
    case Success(lines) => lines.foreach(println)
    case Failure(f) => println(f)
  }

  case class Address(city: String, state: String, zip: String)

  class User(email: String, password: String) {
    //var firstName = None: Option[String]
    var firstName: Option[String] = None
    var lastName = None: Option[String]
    var address = None: Option[Address]
  }

  val u = new User("al@example.com", "secret")
  u.firstName = Some("Al")
  u.lastName = Some("Alexander")
  //u.address = Some(Address("Talkeetna", "AK", "99676"))


  println(u.firstName.getOrElse("<not assigned>"))
  u.address.foreach { a =>
    println(a.city)
    println(a.state)
    println(a.zip)
  }
  println("end")

  val list = List('a', None, 'b')
  for(x <- list){

  }

  val x = list(0) match {
    case 'b' => 1
    case _ => 2
  }

  val x2: java.util.List[Int] = new util.ArrayList[Int]()
  List.range(1,10).foreach(x => x2.add(x))

  //


  val rootSchema = CalciteUtils.registerRootSchema

  val fromworkConfig = Frameworks.newConfigBuilder.parserConfig(SqlParser.Config.DEFAULT).defaultSchema(rootSchema).traitDefs(ConventionTraitDef.INSTANCE, RelDistributionTraitDef.INSTANCE).build

  //val sql = "select u.id as user_id, u.name as user_name, j.company as user_company, u.age as user_age from users u" + " join jobs j on u.id=j.id where u.age > 30 and j.id>10 order by user_id"
  //val sql = "select u.name, sum(duration) from visits v inner join users u on v.id = u.id where v.url like '%google%' and u.age > 30 and v.duration > 10 group by u.name having sum(duration) > 100 order by 2";
  //val sql = "SELECT x,\n       y\nFROM   (SELECT CASE\n                 WHEN u.age > 60 THEN 'old'\n                 WHEN u.age > 30 THEN 'mid'\n                 ELSE 'young'\n               END           AS x,\n               Sum(duration) AS y\n        FROM   visits v\n               INNER JOIN users u\n                       ON v.id = u.id\n        WHERE  v.url LIKE '%google%'\n               AND u.age > 25\n               AND v.duration > 10\n        GROUP  BY CASE\n                    WHEN u.age > 60 THEN 'old'\n                    WHEN u.age > 30 THEN 'mid'\n                    ELSE 'young'\n                  END\n        HAVING Sum(duration) > 100 and Sum(duration) < 200\n        order by 2\n        )\nWHERE  substring(x from 0 for 1) <> 'o' "
  //val sql = "SELECT NAME\n   FROM USERS\n   WHERE EXISTS (\n       SELECT *\n       FROM VISITS\n       WHERE VISITS.ID = USERS.ID\n       AND DURATION > 10)"
  val sql = "SELECT name, first_value(name) over (partition by id order by age) from users"

  //val sql = "SELECT NAME\n   FROM USERS\n   WHERE id > ALL (\n       SELECT id\n       FROM VISITS)"
  // use HepPlanner
  val builder = new HepProgramBuilder
  builder.addRuleInstance(FilterJoinRule.FILTER_ON_JOIN)
  builder.addRuleInstance(ReduceExpressionsRule.PROJECT_INSTANCE)
  builder.addRuleInstance(PruneEmptyRules.PROJECT_INSTANCE)
  builder.addRuleInstance(SubQueryRemoveRule.FILTER)
  builder.addRuleInstance(SubQueryRemoveRule.PROJECT)
  builder.addRuleInstance(SubQueryRemoveRule.JOIN)
  val planner = new HepPlanner(builder.build)

  try {
    val factory = new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT)
    // sql parser
    val parser = SqlParser.create(sql, SqlParser.Config.DEFAULT)
    val parsed = parser.parseStmt
    LOGGER.info("The SqlNode after parsed is:\n{}", parsed.toString)
    val calciteCatalogReader = new CalciteCatalogReader(CalciteSchema.from(rootSchema), CalciteSchema.from(rootSchema).path(null), factory, new CalciteConnectionConfigImpl(new Properties))
    // sql validate
    val validator = SqlValidatorUtil.newValidator(SqlStdOperatorTable.instance, calciteCatalogReader, factory, CalciteUtils.conformance(fromworkConfig))
    val validated = validator.validate(parsed)
    LOGGER.info("The SqlNode after validated is:\n{}", validated.toString)
    val rexBuilder = CalciteUtils.createRexBuilder(factory)
    val cluster = RelOptCluster.create(planner, rexBuilder)
    // init SqlToRelConverter config
    val config = SqlToRelConverter.configBuilder.withConfig(fromworkConfig.getSqlToRelConverterConfig).withTrimUnusedFields(false).withConvertTableAccess(false).withExpand(false).build
    // SqlNode toRelNode
    val sqlToRelConverter = new SqlToRelConverter(new CalciteUtils.ViewExpanderImpl, validator, calciteCatalogReader, cluster, fromworkConfig.getConvertletTable, config)
    var root = sqlToRelConverter.convertQuery(validated, false, true)
    LOGGER.info("after convertQuery:\n{}", RelOptUtil.toString(root.rel))
    root = root.withRel(sqlToRelConverter.flattenTypes(root.rel, true))
    val relBuilder = config.getRelBuilderFactory.create(cluster, null)
    root = root.withRel(RelDecorrelator.decorrelateQuery(root.rel, relBuilder))
    var relNode = root.rel
    LOGGER.info("after flattenTypes:\n{}", RelOptUtil.toString(root.rel))
    planner.setRoot(relNode)
    relNode = planner.findBestExp
    root = root.withRel(relNode)
    LOGGER.info("after hep:\n{}", RelOptUtil.toString(root.rel))
    val relBuilder2 = config.getRelBuilderFactory.create(cluster, null)
    root = root.withRel(RelDecorrelator.decorrelateQuery(root.rel, relBuilder2))
    LOGGER.info("after decorrelateQuery:\n{}", RelOptUtil.toString(root.rel))


  } catch {
    case e: Exception =>
      e.printStackTrace()
  }
}

class GenEntry {

  class Result {

  }


  object RandomUtil {
    val r = new Random()

    def randomString(length: Int): String = {
      val ret = new StringBuilder
      List.range(0, length).foreach {
        ret += r.nextPrintableChar()
      }
      ret.toString()
    }

    def randomInteger(bound: Int): Int = {
      r.nextInt(bound)
    }
  }

  class DataGenVisitor extends RelVisitor with ReflectiveVisitor {

    import org.apache.calcite.rel.core

    import java.math.BigDecimal
    import scala.collection.mutable

    case class Frame(val ordinalInParent: Int, val r: RelNode)
    class ParentStackBottom(cluster: RelOptCluster, traitSet: RelTraitSet) extends AbstractRelNode(cluster, traitSet)



    val dispatcher = ReflectUtil.createMethodDispatcher(classOf[Unit],
      this, "visit", classOf[RelNode]);

    val parentStack = new mutable.Stack[Frame]
    //eros stands for Expected Rel OutputS
    val rel2eros = new mutable.HashMap[RelNode, List[mutable.Map[Int, Any]]]

    //init
    parentStack.push(Frame.apply(0,new ParentStackBottom(null, null)))

    def dispatch(e: RelNode): Unit = dispatcher.invoke(e)

    def visitChild(ordinalInParent: Int, r: RelNode): Unit = try {
      parentStack.push(Frame.apply(ordinalInParent,r))
      dispatch(r)
    } finally parentStack.pop

    def visit(calc: core.Calc): Unit = {


      val rowType = calc.getRowType
      val eros = rel2eros.getOrElseUpdate(calc, createErorsForTopRel(rowType))

      eros.foreach(ero => {
        val ero4child = new mutable.HashMap[Int, Any]()
        ero.foreach(ordinalAndValue =>{

        })
      })


//      //check if current rel can produce such output
//      project.getChildExps.forEach(exp => {
//
//      })

      visitChild(0,calc.getInput)
    }


    var createExpectationForTopRelVisitTimes = 0

    private def createErorsForTopRel(rowType: RelDataType) = {
      createExpectationForTopRelVisitTimes += 1
      assert(createExpectationForTopRelVisitTimes == 1) //should only visit once per visitor

      val ero = new mutable.HashMap[Int, Any]()
      rowType.getFieldList.forEach(field => {
        val randomValue = field.getType.getSqlTypeName match {
          case SqlTypeName.CHAR => RandomUtil.randomString(field.getType.getPrecision)
          case SqlTypeName.INTEGER => new BigDecimal(RandomUtil.randomInteger(100))
          case _ => throw new IllegalStateException()
        }
        ero(field.getIndex) = randomValue
      })
      List(ero)
    }

    def visit(filter: core.Filter): Unit = {

    }
  }

  def foo(rel: RelNode): List[Any] = {

    Nil
  }
}
