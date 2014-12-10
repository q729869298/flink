/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.flink.examples.scala.relational

import org.apache.flink.api.scala._
import org.apache.flink.core.fs.FileSystem.WriteMode

import org.apache.flink.api.java.aggregation.Aggregations

/**
 * This program implements a modified version of the TPC-H query 3. The
 * example demonstrates how to assign names to fields by extending the Tuple class.
 * The original query can be found at
 * [http://www.tpc.org/tpch/spec/tpch2.16.0.pdf](http://www.tpc.org/tpch/spec/tpch2.16.0.pdf)
 * (page 29).
 *
 * This program implements the following SQL equivalent:
 *
 * {{{
 * SELECT 
 *      l_orderkey, 
 *      SUM(l_extendedprice*(1-l_discount)) AS revenue,
 *      o_orderdate, 
 *      o_shippriority 
 * FROM customer, 
 *      orders, 
 *      lineitem 
 * WHERE
 *      c_mktsegment = '[SEGMENT]' 
 *      AND c_custkey = o_custkey
 *      AND l_orderkey = o_orderkey
 *      AND o_orderdate < date '[DATE]'
 *      AND l_shipdate > date '[DATE]'
 * GROUP BY
 *      l_orderkey, 
 *      o_orderdate, 
 *      o_shippriority;
 * }}}
 *
 * Compared to the original TPC-H query this version does not sort the result by revenue
 * and orderdate.
 *
 * Input files are plain text CSV files using the pipe character ('|') as field separator 
 * as generated by the TPC-H data generator which is available at 
 * [http://www.tpc.org/tpch/](a href="http://www.tpc.org/tpch/).
 *
 * Usage: 
 * {{{
 * TPCHQuery3 <lineitem-csv path> <customer-csv path> <orders-csv path> <result path>
 * }}}
 *  
 * This example shows how to use:
 *  - case classes and case class field addressing
 *  - build-in aggregation functions
 * 
 */
object TPCHQuery3 {

  def main(args: Array[String]) {
    if (!parseParameters(args)) {
      return
    }

    // set filter date
    val dateFormat = new java.text.SimpleDateFormat("yyyy-MM-dd")
    val date = dateFormat.parse("1995-03-12")
    
    // get execution environment
    val env = ExecutionEnvironment.getExecutionEnvironment

    // read and filter lineitems by shipDate
    val lineitems = getLineitemDataSet(env).filter( l => dateFormat.parse(l.shipDate).after(date) )
    // read and filter customers by market segment
    val customers = getCustomerDataSet(env).filter( c => c.mktSegment.equals("AUTOMOBILE"))
    // read orders
    val orders = getOrdersDataSet(env)

                      // filter orders by order date
    val items = orders.filter( o => dateFormat.parse(o.orderDate).before(date) )
                      // filter orders by joining with customers
                      .join(customers).where("custId").equalTo("custId").apply( (o,c) => o )
                      // join with lineitems 
                      .join(lineitems).where("orderId").equalTo("orderId")
                                      .apply( (o,l) => 
                                        new ShippedItem( o.orderId,
                                                         l.extdPrice * (1.0 - l.discount),
                                                         o.orderDate,
                                                         o.shipPrio ) )

    // group by order and aggregate revenue
    val result = items.groupBy("orderId", "orderDate", "shipPrio")
                      .aggregate(Aggregations.SUM, "revenue")

    // emit result
    result.writeAsCsv(outputPath, "\n", "|")
    
    // execute program
    env.execute("Scala TPCH Query 3 Example")
  }
  
  // *************************************************************************
  //     USER DATA TYPES
  // *************************************************************************
  
  case class Lineitem(orderId: Long, extdPrice: Double, discount: Double, shipDate: String)
  case class Order(orderId: Long, custId: Long, orderDate: String, shipPrio: Long)
  case class Customer(custId: Long, mktSegment: String)
  case class ShippedItem(orderId: Long, revenue: Double, orderDate: String, shipPrio: Long)

  // *************************************************************************
  //     UTIL METHODS
  // *************************************************************************
  
  private var lineitemPath: String = null
  private var customerPath: String = null
  private var ordersPath: String = null
  private var outputPath: String = null

  private def parseParameters(args: Array[String]): Boolean = {
    if (args.length == 4) {
      lineitemPath = args(0)
      customerPath = args(1)
      ordersPath = args(2)
      outputPath = args(3)
      true
    } else {
      System.err.println("This program expects data from the TPC-H benchmark as input data.\n" +
          " Due to legal restrictions, we can not ship generated data.\n" +
          " You can find the TPC-H data generator at http://www.tpc.org/tpch/.\n" +
          " Usage: TPCHQuery3 <lineitem-csv path> <customer-csv path>" + 
                             "<orders-csv path> <result path>");
      false
    }
  }
  
  private def getLineitemDataSet(env: ExecutionEnvironment): DataSet[Lineitem] = {
    env.readCsvFile[Lineitem](
        lineitemPath,
        fieldDelimiter = '|',
        includedFields = Array(0, 5, 6, 10) )
  }

  private def getCustomerDataSet(env: ExecutionEnvironment): DataSet[Customer] = {
    env.readCsvFile[Customer](
        customerPath,
        fieldDelimiter = '|',
        includedFields = Array(0, 6) )
  }
  
  private def getOrdersDataSet(env: ExecutionEnvironment): DataSet[Order] = {
    env.readCsvFile[Order](
        ordersPath,
        fieldDelimiter = '|',
        includedFields = Array(0, 1, 4, 7) )
  }
  
}
