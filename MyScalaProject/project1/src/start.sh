#!/bin/bash
./scala-2.11.2/bin/scalac -cp $AKKA_HOME/lib/akka/*:$AKKA_HOME/lib/*:. Project1.scala
time ./scala-2.11.2/bin/scala -cp $AKKA_HOME/lib/akka/*:$AKKA_HOME/lib/*:. Project1 "$@"
