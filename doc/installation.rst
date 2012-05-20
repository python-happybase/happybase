************
Installation
************

This guide describes how to install HappyBase.

.. contents:: On this page
   :local:


Setting up a virtual environment
================================

The recommended way to install HappyBase and Thrift is to use a virtual
environment created by `virtualenv`. Setup and activate a new virtual
environment like this:

.. code-block:: sh

   $ virtualenv envname
   $ source envname/bin/activate

If you use the `virtualenvwrapper` scripts, type this instead:

.. code-block:: sh

   $ mkvirtualenv envname


Installing packages
===================

The next step is to install the Thrift package for Python, and HappyBase
itself:

.. code-block:: sh

   (envname) $ pip install thrift happybase

.. note::

   Generating and installing the HBase Thrift Python modules (using ``thrift
   --gen py`` on the ``.thrift`` file) is not necessary, since HappyBase
   bundles pregenerated versions of those modules.


Testing the installation
========================

Verify that the packages are installed correctly by starting a ``python`` shell
and entering the following statements::

   >>> import thrift
   >>> import happybase

If you don't see any errors, the installation was successful. Congratulations!
Now that you have HappyBase installed on your machine, continue with the
:doc:`tutorial <tutorial>` to learn how to use it.


.. vim: set spell spelllang=en:
