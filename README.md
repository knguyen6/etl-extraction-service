# Cloudextraction

Compile the project into a jar file under ```target```.

Then upload to lambda.

## Handler: 
```lambda.Extraction::handleRequest```

## Runtime: 
```Java8```

## Memory(MB): 
```512MB``` at least

### Timeout
depend, start with 1 or 2 mins, could go up to 15 mins.

## Request
Request body required ```bucketname, dbname, tablename, transactionid```


```
{
"bucketname":"my_bucket",
"dbname" : "my_db_records.db",
"tablename" : "sale_table",
"transactionid" : "<passing along>"

}
```

## Response _ example:
```
{
  "uuid": "8f1d0167-e6a2-4aed-870e-17f79948a74f",
  "error": "",
  "vmuptime": 1544408491,
  "newcontainer": 1,
  "success": true,
  "dbname": "my_bucket",
  "fname_filtering": "20181210-022249-e39994fe-edd0-4673-b1f5-8432374caa94-filtering.csv",
  "fname_aggregate": "20181210-022249-e39994fe-edd0-4673-b1f5-8432374caa94-aggregate.csv"
}
```

