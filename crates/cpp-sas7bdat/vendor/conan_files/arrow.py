from conan import ConanFile
import sys
import os
sys.path.insert(0, os.path.dirname(__file__))
from shared import SharedConfig

class ArrowDependency(ConanFile):
    name = "arrow-deps"
    settings = "os", "compiler", "build_type", "arch"  
    requires = "arrow/19.0.1"
    
    def configure(self):
        SharedConfig.apply_options(self)
        