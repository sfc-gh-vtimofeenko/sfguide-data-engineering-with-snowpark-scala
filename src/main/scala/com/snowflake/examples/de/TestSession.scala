package com.snowflake.examples.de

import com.snowflake.examples.utils.WithLocalSession
import com.snowflake.snowpark.Session

/** Simple object to show how the WithLocalSession trait works */
object TestSession extends WithLocalSession {

  // Overridden .execute that receives a session object.
  // This allows passing .execute as a Snowflake stored procedure handler while letting main() method to execute it
  // from the local machine
  def execute(session: Session): String = {
    session.sql("SELECT CURRENT_ACCOUNT()").show()
    session.sql("SELECT CURRENT_ROLE()").show()

    "OK"
  }

  // Note that there is no def main, as WithLocalSession implements it

}
