Redis Resharding Proxy
======================


Function what we can do
---------------------
parse RDB at your host and send command to twemproxy/slave online,Support RDB version: 1 <= version <= 9(not contain stream command).

Special support
---------------------
now, we only support module RedisBloom, we will support another redis module in the feature.

The function has been tested generally, if you find any problems or bugs, please contact me.

Thank you ver much!

Document
---------------------
for most document for RedisBloom, please read https://redis.io/docs/stack/bloom/



Origin
----------------------
fork from https://github.com/smira/redis-resharding-proxy


Testing Data
-----------------------
download from https://github.com/HDT3213/rdb/tree/master/cases

Copyright and Licensing
-----------------------

Copyright 2013 Andrey Smirnov. Unless otherwise noted, the source files are distributed under the MIT License found in
the LICENSE file.







