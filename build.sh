#!/bin/bash
echo -n "Please enter new version: "
read version

CGO_ENABLED=0 GOOS=linux go build -o dbsync2_${version}
GOOS=windows go build -o dbsync2_${version}.exe
GOOS=darwin go build -o dbsync2_${version}_mac
