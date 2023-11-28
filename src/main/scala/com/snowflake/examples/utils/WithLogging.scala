package com.snowflake.examples.utils

import com.typesafe.scalalogging.Logger

trait WithLogging {
  val logger = Logger(getClass.getName)
}
