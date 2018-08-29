## Desc Function Extended

* yearweek

 ```sql
yearweek(date) - Return the Year of the Week value of date. Monday starting point
Example:
  > SELECT yearweek('2013-01-07') FROM table;
  201301
  > SELECT yearweek('2013-01-01') FROM table;
  201253
```

* yearweekdiff

 ```sql
yearweekdiff(yearweek1, yearweek2) - Returns the number of days between yearweek1 and yearweek2
yearweek1 and yearweek2 are strings in the format 'yyyyWeek' or 'yyyy-Week'. 
The time parts are ignored.If yearweek1 is earlier than yearweek2, the result is negative.
Example:
   > SELECT yearweekdiff('201401', '201352') FROM table;
  1
```

* yearmonth

 ```sql
yearmonth(date) - Return the Year and Month value of date
Example:
  > SELECT yearmonth('2013-01-01') FROM table;             
  201301
  > SELECT yearmonth('2013-1-1') FROM table;
  201301
```

* yearmonthdiff

 ```sql
yearmonthdiff(yearmonth1, yearmonth2) - Returns the number of days between yearmonth1 and yearmonth2
yearmonth1 and yearmonth2 are strings in the format 'yyyyMM' or 'yyyy-MM'. 
The time parts are ignored.If yearmonth1 is earlier than yearmonth2, the result is negative.
Example:
   > SELECT yearmonthdiff('201401', '201312') FROM table;
  1
```

* fetch_min

 ```sql
fetch_min(expr, array) - Return the Array of the minimum value of expr
Example:
  id name price
  1  A    100
  2  B    300
  3  C    500

  > SELECT fetch_min(price, array(id, name, price)) FROM item;
  [1, A, 100]
```

* fetch_max

 ```sql
fetch_max(expr, array) - Return the Array of the maximum value of expr
Example:
  id name price
  1  A    100
  2  B    300
  3  C    500

  > SELECT fetch_max(price, array(id, name, price)) FROM item;
  [3, C, 500]
```

* map_sum

 ```sql
map_sum(key, num) - Creates a map the sum numbers with the given key/numbers pairs.
  option map_sum(key, num, accept), map_sum(map<key, num>)
Example:
  id name price
  1  A    100
  2  B    300
  3  A    500

  > SELECT map_sum(name, price) FROM item;
  [A:600, B:300]

  > SELECT map_sum(name, price, if(name = 'A', true, false)) FROM item;
  [A:600]

  > SELECT map_sum(map(name, price)) FROM item;
  [A:600, B:300]
```

* geoip

 ```sql
geoip(ip,property,database)  - looks a property for an IP address froma library loaded
The GeoLite2 database comes separated. To load the GeoLite2 use ADD FILE.
Usage:                                             |
 > geoip(ip, "COUNTRY_NAME", "./GeoLite2-City.mmdb")
Example:
 > SELECT geoip(ip, "COUNTRY_NAME", "./GeoLite2-City.mmdb") FROM table
 > JP
```

* urldecode

 ```sql
urldecode(str) - Returns urldecoded string
Example:
  > SELECT urldecode("http%3a%2f%2fexample%2ecom%2f") FROM table;
  http://example.com/
```
