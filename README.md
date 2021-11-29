## Overview
Flooding substrate node with transactions

To run:

```
# This will install nvm (node version manager)
curl -o- https://raw.githubusercontent.com/nvm-sh/nvm/v0.38.0/install.sh | bash
# Next, we will install node and yarn
nvm install
npm install yarn
# This will generate dist dir which is needed in order for the script to run
npm run build 
# If you would like to use this script with a network simulation tool like 
# "gurke" create a symlink of index.js of this package in a general purpose dir path
ln -s "$(pwd)"/dist/index.js /usr/local/bin/sub-flood
# Run script with
node dist/index.js
```
Example of some more involved command:
```
node --max-old-space-size=4096 dist/index.js --finalization_timeout=20000 --url="ws://localhost:9944" --loops_count=400000 --scale=200 --total_threads=20 --total_transactions=5000 --only_flooding=true --keep_collecting_stats=true --root_account_uri="bla bla bla my secret phrases"
```

## Details of the concept
Implemented script executes the following procedure:
1) Derives temporary accounts from Alice account.
2) Setups these accounts, transfering existential deposit to them.
3) Generates bunch of transactions, transfers from temporary accounts to Alice.
4) Sends generated transactions in batches in several threads every second.
5) Repeats steps from point 3 a given number of times.

In order to measure TPS (transactions per seconds) metric script calculates, how many transactions were included into the blocks
on the latest step (obviously it will be just a part of all sent transactions) and divides on time of operation.

Script may also try to wait for all sent transactions' finalization (see corresponding startup argument). In this case, script
pauses several times, checking every time, if all transactions were finalized.

## Startup arguments
- `total_transactions integer` - total amount of generated transactions, default is 25000
- `scale integer` - scaling parameter for spreading all load among threads, default is 100
- `total_threads integer` - total amount of used threads, default is 10
- `url string` - url to RPC node, default is "ws://localhost:9944"
- `finalization boolean` - boolean flag, if true, script should wait for all transactions finalization, default is false
- `finalization_timeout integer` - amount of time to wait in every attempt, default is 20000 (in ms); script makes several
  attempts, so the total waiting time is finalization_timeout * finalization_attempts
- `finalization_attempts integer` - amount of waiting attempts, default is 5
- `only_flooding boolean` - boolean flag, if true, script will not compute overall statistics after submitting transactions but
  instead immediately attempts to start a new round of flooding
- `keep_collecting_stats boolean` - identifies whether we should be collecting runtime statistics using a background Promise
- `stats_delay integer` - how often background stats collection should invoked, default is 40 seconds (in milliseconds)
- `loops_count integer` - determines how many times the main flooding loop should be executed, default value is `1
- `adhoc_creation boolean` - determines whether transactions should be generated before each execution of the main flooding loop
  or just created in ad-hoc fashion
- `starting_account integer` - all generated accounts are derived an enumeration of seeds from `0` to some given value. This
  parameter, together with the scale parameter (scale maps to number of accounts in general), determines what is the range of
  flooding accounts used during execution. It allows to run this script in parallel on different machines - it requires ones
  to generate flooding accounts in advance.
- `accelerate integer` - determines how many times the main flooding loop should be executed before reaching the given txps.
  Each run accelerates `lineary` towards txps.
- `initial_speed integer` - determines initial `speed` (txps) value of used by the `accelerate` mechanism
