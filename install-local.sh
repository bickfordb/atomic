#!/bin/bash

lein jar
mvn install:install-file -Dfile=atomic-0.1.0-SNAPSHOT.jar -DgroupId=self -DartifactId=atomic -Dversion=0.1 -Dpackaging=jar -DgeneratePom=true
