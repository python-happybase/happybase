***********
Development
***********

.. highlight:: sh

Getting the source
------------------

The HappyBase source code repository is hosted on GitHub:

   https://github.com/wbolster/happybase

To grab a copy, use this::

   $ git clone https://github.com/wbolster/happybase.git



Setting up a development environment
------------------------------------

Setting up a development environment from a Git branch is easy::

   $ cd /path/to/happybase/
   $ mkvirtualenv happybase
   (happybase)$ pip install -r test-requirements.txt
   (happybase)$ pip install -e .


Running the tests
-----------------

The tests use the `nose` test suite. To execute the tests, run::

   (happybase)$ make test

Test outputs are shown on the console. A test code coverage report is saved in
`coverage/index.html`.

If the Thrift server is not running on localhost, you can specify these
environment variables (both are optional) before running the tests::

   (happybase)$ export HAPPYBASE_HOST=host.example.org
   (happybase)$ export HAPPYBASE_PORT=9091

To test the HBase 0.90 compatibility mode, use this::

   (happybase)$ export HAPPYBASE_COMPAT=0.90

To test the framed Thrift transport mode, use this::

   (happybase)$ export HAPPYBASE_TRANSPORT=framed

Contributing
------------

Feel free to report any issues on GitHub. Patches and merge requests are also
most welcome.

.. vim: set spell spelllang=en:
