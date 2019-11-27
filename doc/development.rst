***********
Development
***********

.. highlight:: sh

Getting the source
------------------

The AIOHappyBase source code repository is hosted on GitHub:

   https://github.com/aiudirog/aiohappybase

To grab a copy, use this::

   $ git clone https://github.com/aiudirog/aiohappybase.git



Setting up a development environment
------------------------------------

Setting up a development environment from a Git branch is easy::

   $ cd /path/to/aiohappybase/
   $ python -m venv venv
   $ source venv/bin/activate
   (venv) $ pip install -r test-requirements.txt
   (venv) $ pip install -e .


Running the tests
-----------------

The tests use the `asynctest` test suite. To execute the tests, run::

   (venv) $ make test

Test outputs are shown on the console. A test code coverage report is saved in
`coverage/index.html`.

If the Thrift server is not running on localhost, you can specify these
environment variables (both are optional) before running the tests::

   (venv) $ export AIOHAPPYBASE_HOST=host.example.org
   (venv) $ export AIOHAPPYBASE_PORT=9091

To test the HBase 0.90 compatibility mode, use this::

   (venv) $ export AIOHAPPYBASE_COMPAT=0.90

To test the framed Thrift transport mode (once it is supported), use this::

   (venv) $ export AIOHAPPYBASE_TRANSPORT=framed

Contributing
------------

Feel free to report any issues on GitHub. Patches and merge requests are also
most welcome.

.. vim: set spell spelllang=en:
