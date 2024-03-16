#!/bin/bash
if [ ! -d "build" ]; then
    mkdir build
else
    rm -rf build/*
fi
if [ ! -d "bin" ]; then
    mkdir bin
else
    rm -rf bin/*
fi
if [ ! -d "lib" ]; then
    mkdir lib
else
    rm -rf lib/*
fi
if [ -d "TempDB" ]; then
    rm -rf TempDB
fi
