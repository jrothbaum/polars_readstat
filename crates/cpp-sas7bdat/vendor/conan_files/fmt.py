from conan import ConanFile
import sys
import os
sys.path.insert(0, os.path.dirname(__file__))
from shared import SharedConfig

class FmtDependency(ConanFile):
    name = "fmt-deps" 
    settings = "os", "compiler", "build_type", "arch"
    requires = "fmt/8.0.1"
    
    def configure(self):
        SharedConfig.apply_options(self)
        
    def config_options(self):
        if self.settings.os == "Windows":
            del self.options.fPIC