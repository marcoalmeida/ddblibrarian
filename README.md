# DynamoDB Librarian
This library extends the [Go SDK for DynamoDB](https://docs.aws.amazon.com/sdk-for-go/api/service/dynamodb/) by 
adding support for point-in-time copies of individual items on arbitrary 
DynamoDB tables.

We call these *snapshots*, a form of *item versioning*.


## Overview
`ddblibrarian` is a thin wrapper around the
[Go SDK for DynamoDB](https://docs.aws.amazon.com/sdk-for-go/api/service/dynamodb/)
that adds support for item versioning in the form of *snapshots*.

Each item is tagged with a version identifier in a way that is transparent
to the application &mdash; no code changes are necessary to use this library.

A new snapshot can be started by calling `Snapshot("version-id")`, where
`version-id` is an arbitrary string used to uniquely identify all items 
created thereafter.

The wrappers around the usual `GetItem`, `PutItem`, `UpdateItem`, and `DeleteItem` API calls 
will read/write from/to the *active snapshot* (usually the most recent one).

To work with a specific version of a given item, another set of API calls (carrying the suffix `FromSnapshot`), is 
provided. 


## Core concepts
A *snapshot* is a point in time copy of individual items.
An item exists in the snapshot to which it was written and all future ones.

The *active snapshot* is the point in time copy which API calls use by
default. It defaults to the most recent snapshot, but is updated by calls
to `Rollback` and `Browse`.

A *rollback* changes the active snapshot reverting the DynamoDB table 
to its state at the time the snapshot was taken.

It is also possible to *browse* a given snapshot. This operation changes the active snapshot, but, unlike rolback, it 
does not revert the table's state. The scope of this action is *limited to the client 
session that started it*. 


## Cost
Maintaining multiple versions of each item comes at a cost, both in terms
of storage space and consumed read/write capacity.


The following table lists the overhead of each operation, i.e., extra
throughput capacity consumed that will add to the 
[throughput required](http://docs.aws.amazon.com/amazondynamodb/latest/developerguide/HowItWorks.ProvisionedThroughput.html)
 to read/write an item. 

| Operation     | Overhead       | Notes |
| --------------|----------------|-------|
| `PutItem`     | 1 read unit    ||
| `GetItem`     | 1+N read units   | In the worst case, where N is the number of existing snapshots |
| `GetItemFromSnapshot`     | 1 read unit    ||
| `DeleteItem`     | 1+N read units   | In the worst case, where N is the number of existing snapshots |
| `DeleteItemFromSnapshot`     | 1 read unit    ||

The following operations consume a fixed capacity.

| Operation   | Cost       |
| ------------|----------------|
| `Snapshot`  | 1 read unit + 1 write unit  |
| `Rollback`  | 1 read unit + 1 write unit  |
| `Browse`    | 1 read unit  |


## Limitations
The partition key must be either a string or an integer. No other data types, including floating point, are supported.

Because a snapshot ID requires up to 3 characters, the 
[maximum length](http://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Limits.html)
 of the partition key is reduced to 2045 bytes (or 35 digits, 
if the data type is Number).  


## Work in progress
* `DestroySnapshot`


## Example
Take a look at [the batch job demo](https://github.com/marcoalmeida/ddblibrarian/blob/master/example_batchjob_test.go).
