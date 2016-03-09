.. Note: this list is automatically included in the documentation.

***********************************
To-do list and possible future work
***********************************

This document lists some ideas that the developers thought of, but have not yet
implemented. The topics described below may be implemented (or not) in the
future, depending on time, demand, and technical possibilities.

* Improved error handling instead of just propagating the errors from the
  Thrift layer. Maybe wrap the errors in a HappyBase.Error?

* Automatic retries for failed operations (but only those that can be retried)

* Port HappyBase over to the (still experimental) HBase Thrift2 API when it
  becomes mainstream, and expose more of the underlying features nicely in the
  HappyBase API.
