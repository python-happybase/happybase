"""Load Hbase.thrift as the Hbase_thrift module using thriftpy2 on import."""

from pkg_resources import resource_filename
import thriftpy2

thriftpy2.load(resource_filename(__name__, 'Hbase.thrift'), 'Hbase_thrift')
