import { Keyring } from "@polkadot/keyring";
import { ApiPromise, WsProvider } from "@polkadot/api";
import { KeyringPair } from "@polkadot/keyring/types";
import { BlockHash } from "@polkadot/types/interfaces";

function seedFromNum(seed: number): string {
    return '//user//' + ("0000" + seed).slice(-4);
}

async function getBlockStats(api: ApiPromise, hash?: BlockHash | undefined): Promise<any> {
    const signedBlock = hash ? await api.rpc.chain.getBlock(hash) : await api.rpc.chain.getBlock();

    // the hash for each extrinsic in the block
    let timestamp = signedBlock.block.extrinsics.find(
        ({ method: { method, section } }) => section === 'timestamp' && method === 'set'
    )!.method.args[0].toString();

    let date = new Date(+timestamp);

    return {
        date,
        transactions: signedBlock.block.extrinsics.length,
        parent: signedBlock.block.header.parentHash,
        blockNumber: signedBlock.block.header.number,
    }
}

function transactionBuilder(api: ApiPromise, nonces: number[], keyPairs: Map<number, KeyringPair>, rootKeyPair: KeyringPair, tokensToSend: number): (userNo: number) => any {
    return (userNo: number) => {
        let nonce = nonces[userNo];
        nonces[userNo]++;
        let senderKeyPair = keyPairs.get(userNo)!;

        let transfer = api.tx.balances.transfer(rootKeyPair.address, tokensToSend);
        return transfer.sign(senderKeyPair, { nonce });
    }
}

function incrementalParamsMapper(iterationsLimit: number, threads: number, batches: number, users: number): (iteration: number) => [number, number, number] {
    return function(iteration: number): [number, number, number] {
        if (iteration > iterationsLimit) {
            return [threads, batches, users];
        } else {
            return [threads, batches, Math.ceil(users * iteration / iterationsLimit)];
        }
    }
}

function initializePayload(totalThreads: number, totalBatches: number, txsPerBatch: number, templateTransaction: any): any[][][] {
    let threadPayloads = [];
    for (let thread = 0; thread < totalThreads; thread++) {
        let batches = [];
        for (let batchNo = 0; batchNo < totalBatches; batchNo++) {
            let batch = [...new Array(txsPerBatch)].map((_0, _1) => templateTransaction);
            batches.push(batch);
        }
        threadPayloads.push(batches);
    }
    return threadPayloads;
}

function createIncrementalPayloadBuilder(
    mapParams: (iteration: number) => [number, number, number],
    initialValue: number,
    totalThreads: number,
    totalBatches: number,
    txsPerBatch: number,
    createTransaction: (userNo: number) => any):
(threadPayloads: any[][][]) => Promise<[any[][][], number, number, number]> {

    let iteration = initialValue;
    return async function(threadPayloads: any[][][]): Promise<[any[][][], number, number, number]> {
        if (iteration == initialValue && threadPayloads === undefined) {
            let signedTransaction = createTransaction(0);
            threadPayloads = initializePayload(totalThreads, totalBatches, txsPerBatch, signedTransaction);
        }

        console.log(`Started iteration ${iteration}`);
        let [threads, batches, txsInBatch] = mapParams(iteration);
        iteration += 1;
        return await createPayloadBuilder(threads, batches, txsInBatch, createTransaction)(threadPayloads);
    }
}

function createPayloadBuilder(
    totalThreads: number,
    totalBatches: number,
    usersPerThread: number,
    createTransaction: (userNo: number) => any
): (threadPayloads: any[][][]) => Promise<[ any[][][], number, number, number ]> {

    return async function(threadPayloads: any[][][]): Promise<[ any[][][], number, number, number ]> {
        if (threadPayloads === undefined) {
            let signedTransaction = createTransaction(0);
            threadPayloads = initializePayload(totalThreads, totalBatches, usersPerThread, signedTransaction);
        }
        let sanityCounter = 0;
        for (let thread = 0; thread < totalThreads; thread++) {
            let batches = threadPayloads[thread];
            for (let batchNo = 0; batchNo < totalBatches; batchNo++) {
                let batch = batches[batchNo];
                let ix = 0;
                for (let userNo = thread * usersPerThread; userNo < (thread + 1) * usersPerThread; userNo++) {
                    await (new Promise(async resolve => { resolve(0); }))

                    let signedTransaction = createTransaction(userNo);

                    batch[ix] = signedTransaction;
                    ix++;

                    sanityCounter++;
                }
            }
        }
        console.log(`Done pregenerating transactions (${sanityCounter}).`);
        return new Promise<[any[][][], number, number, number]>(r => r([ threadPayloads, totalThreads, totalBatches, usersPerThread ]));
    };
}

function getOrCreateTransaction(txBuilder: (usernNo: number) => any, threadPayloads: any[][][], thread: number, batch: number, creator: number): any {
    if (threadPayloads === undefined) {
        return txBuilder(creator);
    }
    return threadPayloads[thread][batch][creator];
}

const allSubmittedTxs = new Uint32Array(new SharedArrayBuffer(Uint32Array.BYTES_PER_ELEMENT));

async function executeBatches(
    initialTime: Date,
    threadPayloads: any[][][],
    totalThreads: number,
    totalBatches: number,
    transactionPerBatch: number,
    finalisationTime: Uint32Array,
    finalisedTxs: Uint16Array,
    measureFinalisation: boolean,
    creator: (userNo: number) => any,
) {
    let nextTime = new Date().getTime();
    finalisationTime[0] = 0;
    finalisedTxs[0] = 0;
    const submittedTxs = new Uint32Array(new SharedArrayBuffer(Uint32Array.BYTES_PER_ELEMENT));
    const sentTxs = new Uint32Array(new SharedArrayBuffer(Uint32Array.BYTES_PER_ELEMENT));

    for (let batchNo = 0; batchNo < totalBatches; batchNo++) {

        nextTime = nextTime + 1000;

        console.log(`Starting batch #${batchNo}`);
        let batchPromises = new Array<Promise<any[]>>();
        for (let threadNo = 0; threadNo < totalThreads; threadNo++) {
            batchPromises.push(
                new Promise<any[]>(async resolve => {
                    let errors = [];
                    for (let transactionNo = 0; transactionNo < transactionPerBatch; transactionNo++) {
                        let transaction = getOrCreateTransaction(creator, threadPayloads, threadNo, batchNo, transactionNo);
                        if (measureFinalisation) {
                            let thisResult = 0;
                            await transaction.send(({ status }) => {
                                if (status.isFinalized) {
                                    Atomics.add(finalisedTxs, 0, 1);
                                    let finalisationTimeCurrent = new Date().getTime() - initialTime.getTime();
                                    if (finalisationTimeCurrent > Atomics.load(finalisationTime, 0)) {
                                        Atomics.store(finalisationTime, 0, finalisationTimeCurrent);
                                    }
                                }
                            }).catch((err: any) => {
                                errors.push(err);
                                thisResult = -1;
                            });
                            Atomics.add(sentTxs, 0, 1)
                            if (thisResult == 0) {
                                Atomics.add(submittedTxs, 0, 1);
                                Atomics.add(allSubmittedTxs, 0, 1);
                            }
                        } else {
                            let thisResult = 0;
                            await transaction.send().catch((err: any) => {
                                errors.push(err);
                                thisResult = -1;
                            });
                            Atomics.add(sentTxs, 0, 1)
                            if (thisResult == 0) {
                                Atomics.add(submittedTxs, 0, 1);
                                Atomics.add(allSubmittedTxs, 0, 1);
                            }
                        }
                    }
                    resolve(errors);
                })
            );
        }
        let allErrors = await Promise.all(batchPromises);
        let errors = allErrors.reduce((res, val) => res.concat(val), []);

        if (errors.length > 0) {
            console.error(`${errors.length}/${transactionPerBatch * totalThreads} errors sending transactions`);
        }

        while (new Date().getTime() < nextTime) {
            await new Promise(r => setTimeout(r, 5));
        }

    }
    let submitted = Atomics.load(submittedTxs, 0);
    let allSubmitted = Atomics.load(allSubmittedTxs, 0);
    let sent = Atomics.load(sentTxs, 0);
    console.log(`submitted ${submitted} txn(s) out of ${sent} sent`);
    console.log(`total submitted txs: ${allSubmitted}`);
}

async function collectStats(
    api: ApiPromise,
    initialTime: Date,
    measureFinalisation: boolean,
    finalisationTimeout: number,
    totalTransactions: number,
    finalisationAttempts: number,
    finalisedTxs: Uint16Array,
    finalisationTime: Uint32Array
) {
    let finalTime = new Date();
    let diff = finalTime.getTime() - initialTime.getTime();

    let includedTransactions = 0;
    let totalBlocks = 0;
    let latestBlock = await getBlockStats(api);
    console.log(`latest block: ${latestBlock.date}`);
    console.log(`initial time: ${initialTime}`);
    let prunedFlag = false;
    while (latestBlock.date > initialTime) {
        try {
            latestBlock = await getBlockStats(api, latestBlock.parent);
        } catch (err) {
            console.log("Cannot retrieve block info with error: " + err.toString());
            console.log("Most probably the state is pruned already, stopping");
            prunedFlag = true;
            break;
        }
        if (latestBlock.date < finalTime) {
            console.log(`block number ${latestBlock.blockNumber}: ${latestBlock.transactions} transactions`);
            includedTransactions += latestBlock.transactions;
            totalBlocks++;
        }
    }

    let tps = (includedTransactions * 1000) / diff;

    console.log(`* # of transactions from ${totalBlocks} blocks: ${includedTransactions}`);
    console.log(`* TPS from ${totalBlocks} blocks: ${tps}`);

    if (measureFinalisation && !prunedFlag) {
        let attempt = 0;
        while (true) {
            console.log(`Wait ${finalisationTimeout} ms for transactions finalisation, attempt ${attempt} out of ${finalisationAttempts}`);
            let finalized = Atomics.load(finalisedTxs, 0)
            console.log(`Finalized ${finalized} out of ${totalTransactions}`);
            await new Promise(r => setTimeout(r, finalisationTimeout));

            if (Atomics.load(finalisedTxs, 0) < totalTransactions) {
                if (attempt == finalisationAttempts) {
                    // time limit reached
                    break;
                } else {
                    attempt++;
                }
            } else {
                break;
            }
        }
        console.log(`Finalized ${Atomics.load(finalisedTxs, 0)} out of ${totalTransactions} transactions, finalization time was ${Atomics.load(finalisationTime, 0)}`);
    }
}

async function retrieveTransactionsCount(initialTime: Date, finalTime: Date, api: ApiPromise): Promise<[number, number, Date, Date]> {
    let totalTransactions = 0;
    let totalBlocks = 0;
    let latestBlock = await getBlockStats(api);
    let lastBlockTime = finalTime;
    let lastBlockTimeInitialized = false;
    let firstBlockTime = initialTime;
    while (latestBlock.date > initialTime) {
        firstBlockTime = latestBlock.date;
        try {
            latestBlock = await getBlockStats(api, latestBlock.parent);
        } catch (err) {
            console.log("Cannot retrieve block info with error: " + err.toString());
            console.log("Most probably the state is pruned already, stopping");
            break;
        }
        if (latestBlock.date < finalTime) {
            console.log(`block number ${latestBlock.blockNumber}: ${latestBlock.transactions} transactions`);
            totalTransactions += latestBlock.transactions;
            totalBlocks++;
            if (!lastBlockTimeInitialized) {
                lastBlockTime = latestBlock.date;
                lastBlockTimeInitialized = true;
            }
        }
    }
    return new Promise(resolve => resolve([totalTransactions, totalBlocks, firstBlockTime, lastBlockTime]));
}

async function keepCollectingStats(delay: number, api: ApiPromise) {
    let initialTime = new Date();
    let totalDiff = 0;
    let totalTransactions = 0;
    let totalBlocks = 0;
    while(true) {
        await new Promise(r => setTimeout(r, delay));
        let finalTime = new Date();

        let [transactions, blocks] = await retrieveTransactionsCount(initialTime, finalTime, api);

        let diff = finalTime.getTime() - initialTime.getTime();
        let tps = (transactions * 1000) / diff;
        console.log(`TPS from ${blocks} blocks: ${tps}`);

        totalTransactions += transactions;
        totalBlocks += blocks;
        totalDiff += diff;
        tps = (totalTransactions * 1000) / totalDiff;
        console.log(`TPS from total ${totalBlocks} blocks: ${tps}`);

        initialTime = finalTime;
    }
}

async function run() {
    let argv = require('minimist')(process.argv.slice(2));

    let TOTAL_TRANSACTIONS = argv.total_transactions >= 0 ? argv.total_transactions : 25000;
    let TPS = argv.scale ? argv.scale : 100;
    let TOTAL_THREADS = argv.total_threads ? argv.total_threads : 10;
    let TOTAL_BATCHES = TOTAL_TRANSACTIONS / TPS;
    let WS_URL = argv.url ? argv.url : "ws://localhost:9944";
    let TOTAL_USERS = TPS;
    let USERS_PER_THREAD = TOTAL_USERS / TOTAL_THREADS;
    let TOKENS_TO_SEND = 1;
    let MEASURE_FINALIZATION = argv.finalization ? argv.finalization : false;
    let FINALISATION_TIMEOUT = argv.finalization_timeout ? argv.finalization_timeout : 20000; // 20 seconds
    let FINALISATION_ATTEMPTS = argv.finalization_attempts ? argv.finalization_attempts : 5;
    let ONLY_FLOODING = argv.only_flooding ? argv.only_flooding : false;
    let ROOT_ACCOUNT_URI = argv.root_account_uri ? argv.root_account_uri : "//Alice";
    let KEEP_COLLECTING_STATS = argv.keep_collecting_stats ? argv.keep_collecting_stats : false;
    let STATS_DELAY = argv.stats_delay ? argv.stats_delay : 40000;
    let LOOPS_COUNT = argv.loops_count >= 0 ? argv.loops_count : 1;
    let ADHOC_CREATION = argv.adhoc ? true : false;
    let STARTING_ACCOUNT = argv.starting_account ? argv.starting_account : 0;
    let PEDAL_TO_THE_METAL = argv.accelerate > 0 ? argv.accelerate : 0;
    let INITIAL_SPEED = argv.initial_speed > 0 ? argv.initial_speed : 1;

    let provider = new WsProvider(WS_URL);

    let apiRequest = await Promise.race([
        ApiPromise.create({ provider }),
        new Promise((_, reject) => setTimeout(() => reject(new Error('timeout')), 3000))
    ]).catch(function(err) {
        throw Error(`Timeout error: ` + err.toString());
    });
    let api = apiRequest as ApiPromise;

    let keyring = new Keyring({ type: 'sr25519' });

    let nonces = [];

    console.log("Fetching nonces for accounts...");
    let nonZeroBalance = false;
    for (let i = 0; i < TOTAL_USERS; i++) {
        let stringSeed = seedFromNum(i + STARTING_ACCOUNT);
        let keys = keyring.addFromUri(stringSeed);
        let accountInfo = await api.query.system.account(keys.address);
        let nonce = accountInfo.nonce.toNumber();
        if (!accountInfo.data.free.isZero()) {
            nonZeroBalance = true;
        }
        nonces.push(nonce)
    }
    console.log("All nonces fetched!");

    let rootKeyPair = keyring.addFromUri(ROOT_ACCOUNT_URI);
    let rootNonce = (await api.query.system.account(rootKeyPair.address)).nonce.toNumber();
    console.log("ROOT nonce is " + rootNonce);
    let keyPairs = new Map<number, KeyringPair>()

    for (let seed = 0; seed < TOTAL_USERS; seed++) {
        let keypair = keyring.addFromUri(seedFromNum(seed + STARTING_ACCOUNT));
        keyPairs.set(seed, keypair);
    }

    if (!nonZeroBalance) {
        console.log("Endowing all users from ROOT account...");

        let finalizedTransactions = 0;

        const rootFunds = (await api.query.system.account(rootKeyPair.address)).data.free;
        console.log(`ROOT's funds: ${rootFunds.toBigInt()}`);
        const allAvailableRootFunds = rootFunds.toBigInt() - api.consts.balances.existentialDeposit.toBigInt();
        const partialFeeUpperBound = (await api.tx.balances.transfer(rootKeyPair.address, allAvailableRootFunds).paymentInfo(rootKeyPair)).partialFee.toBigInt();
        const initialBalance = (allAvailableRootFunds / BigInt(TOTAL_USERS)) - partialFeeUpperBound;

        for (let seed = 0; seed < TOTAL_USERS; seed++) {
            let keypair = keyPairs.get(seed);

            let transfer = api.tx.balances.transfer(keypair.address, initialBalance);

            let receiverSeed = seedFromNum(seed);
            console.log(
                `ROOT -> ${receiverSeed} (${keypair.address}) ${initialBalance}`
            );
            await transfer.signAndSend(rootKeyPair, { nonce: rootNonce }, ({ status }) => {
                if (status.isFinalized) {
                    finalizedTransactions++;
                }
            });
            rootNonce++;
        }
        console.log("All users endowed from the ROOT account!");

        console.log("Wait for transactions finalisation");
        await new Promise(r => setTimeout(r, FINALISATION_TIMEOUT));
        console.log(`Finalized transactions ${finalizedTransactions}`);

        if (finalizedTransactions != TOTAL_USERS) {
            throw Error(`Not all transactions finalized`);
        }
    }

    let threadPayloads: any[][][] = undefined;
    let threads: number;
    let batches: number;
    let txsPerBatch: number;
    let nextThreadPayloads: any[][][] = undefined;
    let createTransactionFn = transactionBuilder(api, nonces, keyPairs, rootKeyPair, TOKENS_TO_SEND);
    let payloadBuilder: (threadPayloads: any[][][]) => Promise<[any[][][], number, number, number]> = (threadPayloads: any[][][]) => { return new Promise<[ any[][][], number, number, number ]>(r => r([threadPayloads, TOTAL_THREADS, TOTAL_BATCHES, USERS_PER_THREAD])); };
    if (!ADHOC_CREATION) {
        payloadBuilder = createPayloadBuilder(TOTAL_THREADS, TOTAL_BATCHES, USERS_PER_THREAD, createTransactionFn);
    }
    let paramsMapper: (counter: number) => [number, number, number];
    paramsMapper = function(_: number): [number, number, number] { return [threads, batches, txsPerBatch]; };
    if (PEDAL_TO_THE_METAL > 0) {
        paramsMapper = incrementalParamsMapper(PEDAL_TO_THE_METAL, TOTAL_THREADS, TOTAL_BATCHES, USERS_PER_THREAD);
        payloadBuilder = createIncrementalPayloadBuilder(paramsMapper, INITIAL_SPEED, TOTAL_THREADS, TOTAL_BATCHES, USERS_PER_THREAD, createTransactionFn);
    }
    let nextPayload = payloadBuilder(nextThreadPayloads);
    let submitPromise: Promise<void> = new Promise(resolve => resolve());

    let statsPromise: Promise<void> = new Promise(resolve => resolve());
    if (KEEP_COLLECTING_STATS) {
        statsPromise = new Promise(async resolve => {
            await keepCollectingStats(STATS_DELAY, api);
            resolve();
        });
    }

    let loopsExecuted = 0;
    while (loopsExecuted < LOOPS_COUNT) {
        loopsExecuted += 1;

        console.log(`Pregenerating ${TOTAL_TRANSACTIONS} transactions across ${TOTAL_THREADS} threads...`);
        nextThreadPayloads = threadPayloads;
        [ threadPayloads, threads, batches, txsPerBatch ] = await nextPayload;
        nextPayload = payloadBuilder(nextThreadPayloads);

        console.log("Awaiting for a batch to finish...");
        await (new Promise(async resolve => {
            let initialTime = new Date();
            const finalisationTime = new Uint32Array(new SharedArrayBuffer(Uint32Array.BYTES_PER_ELEMENT));
            const finalisedTxs = new Uint16Array(new SharedArrayBuffer(Uint16Array.BYTES_PER_ELEMENT));

            await executeBatches(initialTime, threadPayloads, threads, batches, txsPerBatch, finalisationTime, finalisedTxs, MEASURE_FINALIZATION, createTransactionFn);
            if (ONLY_FLOODING) {
                resolve(0);
                return;
            }
            await collectStats(api, initialTime, MEASURE_FINALIZATION, FINALISATION_TIMEOUT, TOTAL_TRANSACTIONS, FINALISATION_ATTEMPTS, finalisedTxs, finalisationTime);
            resolve(0);
        }));
        console.log("A batch finished");

    }
    console.log("Awaiting previous batch to finish (disposing)...");
    await submitPromise;
    console.log("Previous batch finished (disposed)");

    await statsPromise;
}

run().then(function() {
    console.log("Done");
    process.exit(0);
}).catch(function(err) {
    console.log("Error: " + err.toString());
    process.exit(1);
});
