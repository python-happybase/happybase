==================
Installation guide
==================

.. highlight:: sh

This guide describes how to install HappyBase.

.. contents:: On this page
   :local:


Setting up a virtual environment
================================

The recommended way to install HappyBase and Thrift is to use a virtual
environment created by `virtualenv`. Setup and activate a new virtual
environment like this::

   $ virtualenv envname
   $ source envname/bin/activate

If you use the `virtualenvwrapper` scripts, type this instead::

   $ mkvirtualenv envname


Installing the HappyBase package
================================

The next step is to install HappyBase. The easiest way is to use `pip` to fetch
the package from the `Python Package Index <http://pypi.python.org/>`_ (PyPI).
This will also install the Thrift package for Python.

::

   (envname) $ pip install happybase

.. note::

   Generating and installing the HBase Thrift Python modules (using ``thrift
   --gen py`` on the ``.thrift`` file) is not necessary, since HappyBase
   bundles pregenerated versions of those modules.


Testing the installation
========================

Verify that the packages are installed correctly::

   (envname) $ python -c 'import happybase'

If you don't see any errors, the installation was successful. Congratulations!


.. rubric:: Next steps

Now that you successfully installed HappyBase on your machine, continue with
the :doc:`user guide <user>` to learn how to use it.


.. vim: set spell spelllang=en:
