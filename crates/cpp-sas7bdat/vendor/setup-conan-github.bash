#!/bin/bash
#echo "conan_home=./.conan2" > .conanrc
conan --version
conan config home
conan profile detect -e
conan profile show
conan remote list
