from os.path import join, dirname
from setuptools import find_packages, setup

__version__ = None
exec(open('aiohappybase/_version.py', 'r').read())


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
    name='aiohappybase',
    version=__version__,
    description="Asyncio fork of HappyBase",
    long_description=get_file_contents('README.rst'),
    author="Roger Aiudi",
    author_email="aiudirog@gmail.com",
    url='https://github.com/aiudirog/aiohappybase',
    install_requires=get_install_requires(),
    packages=find_packages(exclude=['tests']),
    include_package_data=True,
    license="MIT",
    classifiers=(
        "Development Status :: 5 - Production/Stable",
        "Intended Audience :: Developers",
        "License :: OSI Approved :: MIT License",
        "Programming Language :: Python :: 3.6",
        "Programming Language :: Python :: 3.7",
        "Programming Language :: Python :: 3.8",
        "Topic :: Database",
        "Topic :: Software Development :: Libraries :: Python Modules",
    )
)
