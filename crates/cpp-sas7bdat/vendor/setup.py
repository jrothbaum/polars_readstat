#from cmaketools import setup
import os
import logging

import warnings

from cmaketools.cmakebuilder import CMakeBuilder
from cmaketools.cmakecommands import generate_cmdclass
from setuptools import setup as _setup
from cmaketools import cmakeutil
from cmaketools import gitutil

logging.basicConfig(level=logging.DEBUG)


install_requires = [
]

test_requires = [
    'pytest',  # ==3.7.1',
    'pytest-cov', #==2.3.1',
    #'py==1.11.0',
    #'six==1.16.0',
    'setuptools', #==59.8.0',
    #'attrs==21.4.0',
    #'more-itertools==8.12.0',
    #'atomicwrite==1.4.0',
    #'pluggy==1.0.0',
    'coverage', #==6.2',
    #'python-dateutil==2.8.2',
    #'pytz==2021.3',
    'pandas==1.3.5',
    'numpy==1.22.1'
]

def get_cmake_args():
    args = os.getenv('CMAKE_ARGS', "")
    return args.split(' ')

def get_version():
    return open("version.txt", "r").read()
    #with open("CMakeLists.txt", "r") as isf:
    #    for line in isf.readlines():
    #        i = line.find('VERSION')
    #        if i > 0:
    #            return line[i+7:].strip()

def setup(**kwargs):
    # supported keyword arguments to CMakeBuilder constructor
    cmake_keys = (
        "src_dir",
        "test_dir",
        "test_submodules",
        "ext_module_dirs",
        "ext_module_hint",
        "has_package_data",
        "skip_configure",
        "config",
        "generator",
        "toolset",
        "platform",
        "configure_opts",
        "build_opts",
        "install_opts",
        "build_dir",
    )

    # split kwargs into CMakeBuilder arguments and setup arguments
    given_keys = kwargs.keys()
    cmake_args = {key: kwargs[key] for key in given_keys & cmake_keys}
    setup_args = {key: kwargs[key] for key in given_keys - cmake_keys}

    # instantiate CMakeBuilder using its option arguments
    cmake = CMakeBuilder(**cmake_args)

    # create
    setup_args["packages"] = cmake.find_packages()
    setup_args["ext_modules"] = cmake.find_ext_modules()
    setup_args["data_files"] = cmake.get_setup_data_files()
    setup_args["cmdclass"] = {
        **(setup_args["cmdclass"] if "cmdclass" in setup_args else {}),
        **generate_cmdclass(cmake),
    }
    _setup(**setup_args)

setup(
    name="pycppsas7bdat",
    version=get_version(),
    author="Olivia Quinet",
    author_email="olivia.quinet@gmail.com",
    description="A package to read SAS7BDAT file with C++17 and boost::python",
    url="https://github.com/olivia76/cpp-sas7bdat",
    license="Apache License 2.0",
    src_dir="python",
    ext_module_hint=r"Python3_add_library",
    has_package_data=True,
    install_requires=install_requires,
    extras_require={
          'tests': test_requires
    },
    generator="Unix Makefile",
    skip_configure=True,
    build_dir="build/Release",
)
