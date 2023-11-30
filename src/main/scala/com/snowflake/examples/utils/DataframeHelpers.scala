package com.snowflake.examples.utils

import com.snowflake.snowpark.DataFrame
import com.snowflake.snowpark.MergeBuilder
import com.snowflake.snowpark.Updatable

trait DataframeHelpers {

  /** Enahance-my-library pattern for a couple of helper Dataframe functions */
  implicit class DataFrameHelpers(df: DataFrame) {

    /** Prints the dataframe with a specified name and its schema as a tree */
    def printAndPassDf(dfName: String): DataFrame = {
      println(s"$dfName:")
      df.schema.printTreeString()

      df
    }

  }

  implicit class UpdatableHelpers(u: Updatable) {

    // Scala 2 has no Union types, but there are not so many permutations of types here => implemented by hand

    /** Very similar to join(other, usingColumns) -- helps merging Updatables on same columns */
    def merge(other: Updatable, usingColumns: Seq[String]): MergeBuilder = u.merge(
      other,
      (usingColumns
        map (colName => u.col(colName).equal_to(other.col(colName))))
        reduce ((l, r) => l equal_to r)
    )

    def merge(other: Updatable, usingColumn: String): MergeBuilder =
      u.merge(other, u.col(usingColumn).equal_to(other.col(usingColumn)))

    def merge(other: DataFrame, usingColumns: Seq[String]): MergeBuilder = u.merge(
      other,
      (usingColumns
        map (colName => u.col(colName).equal_to(other.col(colName))))
        reduce ((l, r) => l equal_to r)
    )

    def merge(other: DataFrame, usingColumn: String): MergeBuilder =
      u.merge(other, u.col(usingColumn).equal_to(other.col(usingColumn)))

  }

}
