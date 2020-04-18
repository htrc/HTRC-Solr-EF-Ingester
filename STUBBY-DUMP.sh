#!/bin/bash

x=`find json-files-stubby/ -type f`
(for f in $x ; do echo @@@@@@@ $f ; bzcat $f | python -mjson.tool ; done)
