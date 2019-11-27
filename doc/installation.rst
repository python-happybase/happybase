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

   $ python -m venv venv_name
   $ source venv_name/bin/activate

Installing the AIOHappyBase package
===================================

The next step is to install AIOHappyBase. The easiest way is to use `pip` to
fetch the package from the `Python Package Index <http://pypi.python.org/>`_
(PyPI). This will also install the Thrift package for Python.

::

   (venv_name) $ pip install aiohappybase

.. note::

   Generating and installing the HBase Thrift Python modules (using ``thrift
   --gen py`` on the ``.thrift`` file) is not necessary, since HappyBase
   bundles pregenerated versions of those modules.


Testing the installation
========================

Verify that the packages are installed correctly::

   (venv_name) $ python -c 'import aiohappybase'

If you don't see any errors, the installation was successful. Congratulations!


.. rubric:: Next steps

Now that you successfully installed AIOHappyBase on your machine, continue with
the :doc:`user guide <user>` to learn how to use it.


.. vim: set spell spelllang=en:
