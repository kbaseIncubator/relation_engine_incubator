# Graph delta batch load atomicity

## Rollbacks

Note that some of these options do not specifically enable roll backs, but an update can be rolled
back (unatomically) by:

* Setting any expirations dates equal to `timestamp - 1` to ∞.
* Deleting any nodes or edges where the `created` field equals `timestamp`, including merge edges.
* Setting `last_version` to the prior load version on any nodes or edges where `last_version`
  equals the load version.

If the `timestamp` is in the future at the time the rollback completes, it's as if the update never
happened. If not, then any queries between the `timestamp` and the completion of the rollback may
be spurious.

## Option 1: Reject queries

* While an upgrade is in progress reject RE queries.
  * This is reasonable if updates are quick and can be done at times of low load.
  * This may cause apps to fail or views to not render.
  * Note that only queries after `timestamp - 1` need to be rejected while the update is in
    progress.
  * Include a useful error message explaining that the DB is unavailable temporarily.

## Option 2: Queue queries

* While an upgrade is in progress queue RE queries until the update is complete.
  * This is reasonable if updates are quick.
  * This may cause apps or views to wait.
  * Note that only queries after `timestamp - 1` need to be queued while the update is in
    progress.

## Option 3: Far future timestamp and faith

* When starting the update, set the `timestamp` to > expected_update_time * 4.
* The update proceeds as normal.
* If the update fails, system administrators have until `timestamp` to rollback the upgrade. If
  they are unsuccessful, queries after `timestamp` may be spurious.
  * The system could be brought down prior to `timestamp` if necessary.

## Option 4: Restricted timestamps
* While the update is in progress:
  * Any query with a timestamp &lt; `timestamp - 2` proceeds normally.
  * Any other query's behavior depends on an input parameter `strict_timestamp`:
    * If `false`, the input timestamp is substituted by `timestamp - 2` by the RE API.
      This timestamp is returned in the query as the timestamp that was used.
    * If `true` an error is thrown.

## Option 5: Red / Green expire fields

* Nodes and edges now have two expiration fields, one of which is active and one of which is
  inactive at any given time.
  * Names for these fields should not inticate that either is inactive/active as they will
    switch over time.
  * The name of the currently active expiration field is stored in the database and used for all
    queries.
* Data sets (which may include more than one collection, typically nodes + edges,
  e.g. NCBI Taxonomy) are in one of four states, in the order given, at any given time:
  * `Clean` - both expiration fields on each node / edge (NE) are equal.
  * `Updating` - data updates are being applied to the data set on the inactive edges.
  * `Wait switch` - data updates are complete but the active expire field has not been
    switched.
  * `Sync expire` - the active expire field has been switched and the values of the inactive field
    are being set to the value of the active field for each NE. One all inactive fields are updated
    the state returns to `Clean`.
* The timestamp supplied for the time when the update becomes active must be far enough in the
  future that active edge is switched prior to the timestamp.
* The update starts from the `Clean` state and proceeds as follows:
  * The state is changed to `Updating`.
  * The update is applied to the data set, *affecting only inactive expiration fields*.
    * If new NEs are required, the active expiration field is set to -∞.
      * This prevents the new NEs from appearing in current queries.
  * At this point the update may be removed by deleting all NEs with active expiration fields
    where the value is -∞ and setting the inactive expiration fields equal to the active fields.
    * This may be useful if an update fails and must be restarted.
  * The state is changed to `Wait switch`.
  * The active expiration field is switched to the currently inactive field.
    * The update timestamp **MUST** be after the current time to prevent a retroactive update.
      * If the timestamp is not in the future, the upgrade must be removed and restarted with a
        new timestamp.
    * At this point the update is now active.
  * Wait for some amount of time to ensure any in flight queries are complete.
  * Change the state to `Sync expire`.
  * Update all NEs such that the inactive expiration field is equal to the active field.
  * Change the state to `Clean`.
  * The data set is now ready for a new update to be applied.

### Indexes

* Any index that includes the expired field must now be duplicated for both expire fields.
* Indexes are required on both expired fields to find nodes with a -∞ expiration time when
  removing an incomplete update.

### Muliple data sets

The procedure above has drawbacks when queries may span multiple batch loaded data sets with
expiration fields:

* Each data set must have its own active expiration field, and the data sets may have different
  active fields based on the number of updates that have been performed.
* If a query spans a data set implicitly, it is impossible to inform the query which expiration
  field to use in that data set.

A global active expiration field can be used to resolve these issues at the cost of more
tightly coupled loads:

* Loads may no longer proceed independently, but must be coordinated with each other.
* There is only one expiration field active at any given time across all data sets.
  * The two expiration fields have the same name in all data sets.
* Prior to starting an update, all data sets must be in the `Clean` state.
* The active expiration field cannot be switched until all planned updates are in 
  `Wait switch`.
* Starting new updates on collections in the `Clean` state must be prevented prior to switching
  the active expiration field.
  * One possible method is moving the state of all collections from `Clean` to `Wait switch`.
    Updates cannot then start.
* **ALL** load timestamps must be in the future.
  * If such is not the case, all updates must be removed and restarted.
* The active expiration field can then be switched, and all updates are now active.
* Syncing for all updated collections proceeds as normal.

## Option 6: Red / Green collections:

* Create new nodes and edges collections (red).
* Blacklist the new collections from any queries.
* Copy the current collections (green) into the red collections.
* Perform the update on the red collections.
* Updating edges from external collections can proceed in one of two ways:
    * Halting
      * Halt updates to external collections
      * Copy all current edges to the new collection
      * Resume updates when the red -> green switch occurs (below)
    * Dual update
    * Add new edges to both the green and red collections
    * This runs the risk of leaving the db in an inconsistent state if an update fails
        for one collection but not the other
        * Need to consider how to restart the update from a case like this
        * Which collection gets the update first?
    * Stop adding updates to the green collection after the red -> green switch
* When updates are complete, blacklist the green collections and remove the blacklist for
    the red collections
    * Ideally atomically - in ElasticSearch this is possible with aliases, for example
      * In Arango there doesn't seem to be an easy way to do this - no collection aliasing or
        renaming
* Delete the green collections
* Change the red collection to green

This adds significant complexity to the update process.