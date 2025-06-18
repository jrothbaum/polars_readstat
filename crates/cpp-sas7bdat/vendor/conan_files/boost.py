from conan import ConanFile
import sys
import os
sys.path.insert(0, os.path.dirname(__file__))
from shared import SharedConfig

class BoostDependency(ConanFile):
    name = "boost-deps"
    settings = "os", "compiler", "build_type", "arch"
    requires = "boost/1.85.0"
    
    def configure(self):
        SharedConfig.apply_options(self)
        
    def config_options(self):
        if self.settings.os == "Windows":
            del self.options.fPIC