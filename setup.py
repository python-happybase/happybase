from os.path import join, dirname
from setuptools import find_packages, setup

execfile('happybase/version.py')

setup(name='happybase',
      version=__version__,
      description="A developer-friendly Python library to interact "
                  "with Apache HBase",
      long_description=open(join(dirname(__file__), 'README.rst')).read(),
      author="Wouter Bolsterlee",
      author_email="uws@xs4all.nl",
      url='https://github.com/wbolster/happybase',
      install_requires=['thrift'],
      packages=find_packages(),
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
