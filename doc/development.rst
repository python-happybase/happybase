
***********
Development
***********

The HappyBase source code repository is hosted on GitHub:

   https://github.com/wbolster/happybase

Feel free to report issues. Patches are also most welcome.


Test suite
----------

The tests use the `nose` test suite. To execute the tests, run:

.. code-block:: sh

   $ make test

Test outputs are shown on the console. A test code coverage report is saved in
`coverage/index.html`.

If the Thrift server is not running on localhost, you can specify these
environment variables (both are optional) before running the tests:

.. code-block:: sh

   $ export HAPPYBASE_HOST=host.example.org
   $ export HAPPYBASE_PORT=9091

To test the HBase 0.90 compatibility mode, use this:

.. code-block:: sh

   $ export HAPPYBASE_COMPAT=0.90

.. vim: set spell spelllang=en:
