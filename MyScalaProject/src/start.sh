#!/bin/bash
scalac -cp $AKKA_HOME/lib/akka/*:$AKKA_HOME/lib/*:. Project1.scala
time scala -cp $AKKA_HOME/lib/akka/*:$AKKA_HOME/lib/*:. Project1 "$@"
