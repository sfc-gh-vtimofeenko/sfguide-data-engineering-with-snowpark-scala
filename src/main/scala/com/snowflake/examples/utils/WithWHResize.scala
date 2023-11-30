package com.snowflake.examples.utils

import scala.util.Success
import scala.util.Try

trait WithWHResize {

  /** Upsizes the warehouse before the warehouse is executed and downsizes it after function is complete
    *
    * @param session
    *   a Snowpark session
    * @param f
    *   wrapped function
    * @return
    *   output of f wrapped into [[scala.util.Success]]
    */
  def withWHResize[T](session: Session, f: => T): T = {
    // Not using getEnv since we don't want to change the size of a random warehouse
    session sql "ALTER WAREHOUSE SCALA_DE_WH SET WAREHOUSE_SIZE = 'XLARGE' WAIT_FOR_COMPLETION = TRUE" collect ()

    val res: T =
      try f
      finally session sql "ALTER WAREHOUSE SCALA_DE_WH SET WAREHOUSE_SIZE = 'XSMALL'" collect ()

    res
  }

}
