package com.supergloo

import java.util.Properties

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

/**
  * This example assumes a mysql database called "employees"
  * It is a free sample mySQL database available and loaded
  * with the test_db found here
  * https://github.com/datacharmer/test_db.git
  */
object ETLExample {

  def main(args: Array[String]) {

    // TODO use config instead of hard code
    val conf = new SparkConf().setAppName("Skeleton")
    conf.setIfMissing("spark.master", "local[*]")
    conf.setIfMissing("spark.cassandra.connection.host", "localhost")

    val spark = SparkSession
      .builder()
      .config(conf)
      .getOrCreate()

    import spark.sqlContext.implicits._

    val url = "jdbc:mysql://localhost/employees"
    val props = new Properties()
    props.put("user", "root")
    props.put("password", "root")
    props.put("driver", "com.mysql.jdbc.Driver")

    println("Creating aggregates")
    /*
    There are a few ways we could load data from a JDBC
    source.  Here's one way and there's another way commented out below as well

    You'd want to monitor and measure in Spark UI to determine best approach
    from a performance standpoint
     */
    // begin default example of loading from a JDBC source

    val query = """(SELECT d.dept_name as department, avg(s.salary) as average, min(s.salary) as minimum, max(s.salary) as maximum
                  FROM employees as e
                  INNER JOIN salaries as s
                  ON s.emp_no = e.emp_no
                  INNER JOIN dept_emp as de
                  ON e.emp_no = de.emp_no
                  INNER JOIN departments as d
                  ON d.dept_no= de.dept_no group by d.dept_name) e"""

    val df = spark.read.jdbc(url, query, props).as[DeptAggregates]

    df.cache()
    df.collect.map { a => println(a)}
    df.collect.map {a => println(s"NOW: ${a.average + 100}")}

    // end default example

    /*
    Here's another example of loading from JDBC
    */

  /*  val tables = List("employees", "salaries", "departments", "dept_emp")
    for {
      table <- tables
    } spark.read.jdbc(url, table, props).createOrReplaceTempView(table)

    val aggs = spark.sql("""SELECT d.dept_name as department, avg(s.salary) as average,
                             min(s.salary) as minimum, max(s.salary) as maximum
                             FROM employees as e
                             INNER JOIN salaries as s
                             ON s.emp_no = e.emp_no
                             INNER JOIN dept_emp as de
                             ON e.emp_no = de.emp_no
                             INNER JOIN departments as d
                             ON d.dept_no= de.dept_no group by d.dept_name""").as[AnotherExampleAggregate]

    aggs.cache()
    aggs.collect.map { a => println(a)}
    aggs.collect.map {a => println(s"NOW: ${a.average + 100}")}
    */

    // write default approach of JDBC of aggregated results Cassandra
    // verify cassandra connection
    println(s"Cassandra connection: ${spark.conf.get("spark.cassandra.connection.host")}")


    /* Assumes test_keyspace and employee_new table */
    /*
    CREATE KEYSPACE test_keyspace WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1 };
    USE test_keyspace;
    CREATE table employee_new (
        department  TEXT,
        average  double,
        minimum double,
        maximum double,
        PRIMARY KEY (department));
     */
    df.write
      .format("org.apache.spark.sql.cassandra")
      .mode("overwrite")
      .options(Map( "table" -> "employee_new", "keyspace" -> "test_keyspace"))
      .save()

    println("finished writing to cassandra")
    spark.stop()
    sys.exit(0)
  }
}

case class DeptAggregates(department: String, average: BigDecimal, minimum: BigDecimal, maximum: BigDecimal)
case class AnotherExampleAggregate(department: String, average: Double, minimum: Double, maximum: Double)