# zeppelin-interpreter
Snappydata interpreter for Apache Zeppelin

# Snappydata Interpreter for Apache Zeppelin

## Overview
[SnappyData](http://snappydatainc.github.io/snappydata/) is a distributed in-memory data store for real-time operational analytics, delivering stream analytics, OLTP (online transaction processing) and OLAP (online analytical processing) in a single integrated cluster. We realize this platform through a seamless integration of Apache Spark (as a big data computational engine) with GemFire XD (as an in-memory transactional store with scale-out SQL semantics).

## Installing and Running Snappydata Interpreter:
Snappydata cluster internally starts zeppelin interpreter on the lead node so in order to use Snappydata interpreters in zeppelin, you have install Snappydata cluster using simple steps as mentioned in the following [here](<TODO: Add installation docs link here>):
  Once snappydata cluster is being set up you have to connect to snappydata interpreter from zeppelin server.Following are the steps used to connect to remotely executing snappydata interpreter from zeppelin server:
  1. Login to the zeppelin UI and browse for interpreter settings
  2. Search for snappydata interpreter settings.Click edit the snappydata interpreter and then select "Connect to existing process"
  3. Specify host on which snappydata lead node is executing along with the snappydata zeppelin port (Default: 3768) 
  4. Edit the other properties if needed.Following image shows snappydata interpreter properties:
  ![Snappydata Interpreter settings](images/snappydata_interpreter_properties.png)

## Supported properties and their values:
  First Header | Second Header | Description |
  ------------ | -------------| ------------ |
  default.driver  | Content from cell 2 | 
  snappydata.store.locators   | localhost:10334  | Used to specify locator URI (only **local/split** mode)|
  master | local[*] | Used to specify spark master URI (only **local/split** mode)|
  zeppelin.jdbc.concurrent.use | true | Used to specify which zeppelin scheduler should be used.True for Fair and False for FIFO |

## Using snappydata zeppelin interpreter:
  Snappydata Zeppelin Interpreter group consist of 2 interpreters:
  Interpreter Name | Description |
  ---------------- | ----------- |
  %snappydata.snappydata | This interpreter is used to write scala code on the paragraph.SnappyContext is injected in this interpreter and can be accessed using variable **_snc_**
  %snappydata.sql | This interpreter is used to execute sql queries on snappydata cluster. It also has features of executing Approximate queries on snappydata cluster.(Refer examples)

### Examples of Using Snappydata zeppelin Interpreter
