# MemcLoad


### MemcLoad

Parse and load to memcache applications logs.

### Requirements

 - Python 3.0 or later
 - python-memcached
 - protobuf

### Using

Run script:

```
python3  memc_load.py 
```  

optional arguments:

```      
	-t, --test       run protobuf test mode
	-l, --log        log path
	--dry            run debug mode
	--pattern        log path pattern, default value:"/data/appsinstalled/*.tsv.gz"
	--idfa           idfa server address, default value: "127.0.0.1:33013"
	--gaid           gaid server address, default value: "127.0.0.1:33014"
	--adid           adid server address, default value: "127.0.0.1:33015"
	--dvid           dvid server address, default value: "127.0.0.1:33016"
```





