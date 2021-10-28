<!--
Copyright (c) 2014 - 2021 YCSB contributors. All rights reserved.

Licensed under the Apache License, Version 2.0 (the "License"); you
may not use this file except in compliance with the License. You
may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
implied. See the License for the specific language governing
permissions and limitations under the License. See accompanying
LICENSE file.
-->

## Quick Start

This section describes how to run YCSB on NDB. 

### 1. Start NDB

### 2. Create Table

Create the following table in a database. Default DB name is ycsb which you can override with 
`ndb.schema` property  .

```sql
CREATE TABLE `usertable` (  `key` varchar(255) NOT NULL,  `field0` varchar(255) DEFAULT NULL,  `field1` varchar(255) DEFAULT NULL,  `field2` varchar(255) DEFAULT NULL,  `field3` varchar(255) DEFAULT NULL,  `field4` varchar(255) DEFAULT NULL,  `field5` varchar(255) DEFAULT NULL,  `field6` varchar(255) DEFAULT NULL,  `field7` varchar(255) DEFAULT NULL,  `field8` varchar(255) DEFAULT NULL,  `field9` varchar(255) DEFAULT NULL,  PRIMARY KEY (`key`)  ) 
```
### 2. Install Java and Maven



### 3. Set Up YCSB

Git clone YCSB and compile:

    git clone http://github.com/brianfrankcooper/YCSB.git
    cd YCSB
    mvn -pl site.ycsb:ndb-binding -am clean package

### 4. Provide NDB Connection Parameters
    
Set host, port, password, and cluster mode in the workload you plan to run. 

- `ndb.host`  Default : 127.0.0.1
- `ndb.port`  Default : 1186
- `ndb.schema`  Default : ycsb 

Or, you can set configs with the shell command, EG:

    ./bin/ycsb load ndb -s -P workloads/workloada
    OR
    ./bin/ycsb load ndb -s -P workloads/workloada -p "ndb.host=127.0.0.1" -p "ndb.port=1186" -p "ndb.schema=ycsb" > outputLoad.txt

### 5. Load data and run tests

Load the data:

    ./bin/ycsb load ndb -s -P workloads/workloada > outputLoad.txt

Run the workload test:

    ./bin/ycsb run ndb -s -P workloads/workloada > outputRun.txt

