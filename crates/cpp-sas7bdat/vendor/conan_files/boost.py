from conan import ConanFile
import sys
import os
sys.path.insert(0, os.path.dirname(__file__))
from shared import SharedConfig

class BoostDependency(ConanFile):
    name = "boost-deps"
    settings = "os", "compiler", "build_type", "arch"
    requires = "boost/1.82.0"
    
    def configure(self):
        SharedConfig.apply_options(self)
        