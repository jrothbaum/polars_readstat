from conan import ConanFile
import sys
import os
sys.path.insert(0, os.path.dirname(__file__))
from shared import SharedConfig

class SpdlogDependency(ConanFile):
    name = "spdlog-deps"
    settings = "os", "compiler", "build_type", "arch"
    requires = "spdlog/1.9.2"
    
    def configure(self):
        SharedConfig.apply_options(self)
        
    def config_options(self):
        if self.settings.os == "Windows":
            del self.options.fPIC