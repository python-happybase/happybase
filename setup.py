from os.path import join, dirname
from setuptools import find_packages, setup

execfile('happybase/_version.py')


def readfile(filename):
    with open(join(dirname(__file__), filename)) as fp:
        return fp.read()

description = readfile('README.rst') + "\n\n" + readfile("NEWS.rst")

setup(name='happybase',
      version=__version__,
      description="A developer-friendly Python library to interact "
                  "with Apache HBase",
      long_description=description,
      author="Wouter Bolsterlee",
      author_email="uws@xs4all.nl",
      url='https://github.com/wbolster/happybase',
      install_requires=['thrift'],
      packages=find_packages(exclude=['tests']),
      license="MIT",
      classifiers=(
          "Development Status :: 4 - Beta",
          "Intended Audience :: Developers",
          "License :: OSI Approved :: MIT License",
          "Programming Language :: Python :: 2",
          "Topic :: Database",
          "Topic :: Software Development :: Libraries :: Python Modules",
          )
      )
