from os.path import join, dirname
from setuptools import find_packages, setup

__version__ = None
execfile('happybase/_version.py')


def get_file_contents(filename):
    with open(join(dirname(__file__), filename)) as fp:
        return fp.read()


def get_install_requires():
    requirements = get_file_contents('requirements.txt')
    install_requires = []
    for line in requirements.split('\n'):
        line = line.strip()
        if line and not line.startswith('-'):
            install_requires.append(line)
    return install_requires


setup(
    name='happybase',
    version=__version__,
    description="A developer-friendly Python library to interact with "
                "Apache HBase",
    long_description=get_file_contents('README.rst'),
    author="Wouter Bolsterlee",
    author_email="uws@xs4all.nl",
    url='https://github.com/wbolster/happybase',
    install_requires=get_install_requires(),
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
