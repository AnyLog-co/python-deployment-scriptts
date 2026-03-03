# Aggregation

Details regarding aggregation can be found in the <a href="https://github.com/AnyLog-co/documentation/blob/master/aggregations.md" target="_blank">documentation</a>

Aggregation functions summarize streaming data over defined time intervals. For each table, users configure the 
interval duration and the number of intervals to retain, enabling continuous, rolling aggregations.

When aggregations there's a need to configure the logic for ingesting the data as it passes through aggregation:

* **Option 1**: Not save the data at all 
* **Option 2**: Save the raw data as it comes in
* **Option 3**: Save the summary or aggregation results of the data 

Options 2 and 3 can coexist by simply enabling both `source` and `derived` as true when executing 
`set aggregation ingest`. 

## Python Script

The python code is a multi-step process, but allows for more comprehensive manipulation of aggregating data.

1. Start an AnyLog/EdgeLake agent that's able to update the blockchain 
 
2. attach to the executable of the container
```shell
make exec ANYLOG_TYPE=[agent name]
```

3. Execute [aggregation.py](aggregation.py) - based on the options the python3 script would declare aggregation policy/policies
and initiate them (if option is set)

**Example**: Declare policy only
```shell
python3 /app/deployment-scripts/aggregation/aggregation.py 127.0.0.1:32049 \
  --db-name demo \
  --table-name t1 \
  --timestamp-column timestamp \
  --value-columns val1,val2,val3 \
  --intervals 10 \
  --intervals-time 1 minute \
  --keep-source \
  --declare-policy-only
```

**Example**: Unique aggregation per value column
```shell
python3 /app/deployment-scripts/aggregation/aggregation.py 127.0.0.1:32149 \
  --db-name demo \
  --table-name t1 \
  --timestamp-column timestamp \
  --value-columns val1 \
  --intervals 10 \
  --intervals-time 1 minute \
  --keep-source 

python3 /app/deployment-scripts/aggregation/aggregation.py 127.0.0.1:32149 \
  --db-name demo \
  --table-name t1 \
  --timestamp-column timestamp \
  --value-columns value2 \
  --intervals 10 \
  --intervals-time 1 minute \
  --keep-aggregation \
  --encoding-type arle
  
python3 /app/deployment-scripts/aggregation/aggregation.py 127.0.0.1:32149 \
  --db-name demo \
  --table-name t1 \
  --timestamp-column timestamp \
  --value-columns value3 \
  --intervals 10 \
  --intervals-time 1 minute \
  --keep-source \
  --keep-aggregation \
  --encoding-type bounds
```

4. Accepting data - our suggestion is to start accepting data **after** aggregation is configured; especially when 
`--keep-aggregation` is set to _true_. The reason why is because the table definition (ie `CREATE` statement) may alter 
based on the aggregation configurations


## Configuration based

The deployment scripts also have the ability to run aggregation, though they are much less comprehensive; 
meaning they are not written such that users can define multiple tables or multiple columns per table. 

### Using the configurations

In the advanced configs file, update the aggregation configurations, this would then run [aggregation.al](../sample-scripts/aggregation.al)
script.

### Using Policies 

**Option 1**: Single policy with aggregation information - simply specify the ID in `AGGREGATION_POLICY` 
(advanced configs) and the agent will automatically run the configurations. 

**Option 2**: Update the [local_script.al](../node-deployment/local_script.al) file with `config from policy` call per
aggregation policy. Note, `local_script.al` is the **last** step in configurations. 






