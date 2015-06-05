sslsessionpool
==============

SSL session ID caching implementation for Go using memcache.  Allows you to
specify multiple memcache pools (for multiple datacenters) and it will try them
in turn.
