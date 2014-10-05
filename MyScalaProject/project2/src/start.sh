#!/bin/bash
./scala-2.11.2/bin/scalac -cp $AKKA_HOME/lib/akka/*:$AKKA_HOME/lib/*:. Project2.scala
./scala-2.11.2/bin/scala -cp $AKKA_HOME/lib/akka/*:$AKKA_HOME/lib/*:. Project2 "$@"
