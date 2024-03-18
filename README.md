* ErgoValidation

Program that validates transactions using sigma-rust. Requires a node running at port 9053 with ergo.node.extraIndex = true. When validating transactions, failures will be logged to failures.txt. 
You can also inspect validation.log for more detailed information, including the time it takes to validate transactions 

* Validating individual transactions

You can validate individual transactions using validate-transaction like so:
```shell
cargo run --release -- validate-transaction 7f838c5060185125cb554323401d87c0fc135a0c5fc545f9e315ffc38b97b06e
```
