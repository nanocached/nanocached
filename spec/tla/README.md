# TLA+ model of the join handoff

`JoinHandoff.tla` models one key during a staged join: the key's old owners,
the joiner, one client writing one op at a time, the join's put-if-absent
transfer and each old owner's forward queue to the joiner. The header of the
spec maps each action to the code it stands for and lists what is left out.

A run that finds no violation means only that this model, within its bounds,
has none. It says nothing about the code paths the model leaves out, or about
places where the model and the code differ.

## Running

TLC from the `tla2tools.jar` of the v1.7.4 release of tlaplus/tlaplus
(Java 11 or later):

```sh
java -cp /path/to/tla2tools.jar tlc2.TLC -deadlock \
  -metadir /tmp/tlc-meta -config fixed_r2.cfg JoinHandoff.tla
```

`-deadlock` turns off the deadlock check: the model stops on purpose once the
client has made its last op. `-metadir` keeps TLC's state files out of the
tree.

## Configurations

| Config | Expected | What it checks |
|---|---|---|
| `fixed_r1`, `fixed_r2` | no violation | all fixes in, R = 1 and R = 2, ops start only before the promotion (their legs may land after it) |
| `post_confirm_r1`, `post_confirm_r2` | no violation | the same, with writes by the old or the new list after the promotion |
| `bug_no_delete_tombstone` | `TransferCopyNotStale` violated | a transfer copy read before a delete lands after the forwarded delete |
| `bug_no_expiry_tombstone` | `TransferCopyNotStale` violated | the same with a touch to TTL 0 |
| `bug_sweep_before_confirm` | `AbandonKeepsCopies` violated | a dead copy swept at completion, then the join abandoned |
| `accepted_lagging_forward` | `JoinerHoldsFinalStrict` violated | accepted: a queued forward overwrites a newer direct write on the joiner |
| `accepted_swallowed_replica_w` | `JoinerHoldsFinal` violated | accepted: a replica leg refused with W is dropped, and another old owner's older forward lands last |
