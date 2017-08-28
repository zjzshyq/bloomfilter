# bloomfilter
This is a bloom filter in python! This project can filter pairs like (query_id,value)!
I build a key for every query_id in redis using bitmap, and all the pairs are stored in mongodb. The bitmap will expand when insert number is large.
