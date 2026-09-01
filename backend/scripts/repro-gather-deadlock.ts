/**
 * Reproduces the permanent device-init loop (init-slot STUCK … stage=build)
 * with no hardware and no network — see issues #28 and #30.
 *
 * What a device on a bad link does, and what this script recreates:
 *
 *   1. The waiting-room gather starts. Shelly.GetDeviceInfo / GetStatus /
 *      GetConfig answer; ListMethods is still in flight when the link drops.
 *   2. `ws` closes the socket, so WebSocketTransport tears down and
 *      RpcTransport.destroy() rejects the pending RPCs — and clears the
 *      stale-message sweeper.
 *   3. getData() only WARNS on a ListMethods rejection, so the gather keeps
 *      going with `methods = []`. That skips the component probe and lands
 *      straight on Webhook.ListAllSupported.
 *   4. That send registers a pending RPC on the dead transport. ws.send() is
 *      a silent no-op once closed (no callback → sendAfterClose does nothing),
 *      so nothing ever resolves it, and the sweeper that would have timed it
 *      out is gone.
 *   5. The gather promise never settles, so SingleFlight never clears its
 *      entry. Every later connection dedups onto it and sends no RPCs at all,
 *      until the 90s init-slot watchdog reclaims — forever. Only a process
 *      restart clears it.
 *
 * Run it:
 *   npx tsx backend/scripts/repro-gather-deadlock.ts
 *
 * On unpatched code it reports DEADLOCK; with the gather deadlines in place
 * it recovers unattended. Override FM_DEVICE_INIT_PROBE_TIMEOUT_MS /
 * FM_WAITING_GATHER_MAX_MS to make it finish faster.
 */

import './bench-events-env';
import {EventEmitter} from 'node:events';
import ShellyDeviceFactory from '../src/model/ShellyDeviceFactory';
import WebSocketTransport from '../src/model/transport/WebsocketTransport';
import {
    clearGatheredDataForTests,
    gatherDeviceDataOnce,
    takeGatheredData
} from '../src/modules/deviceIngress/gatheredDeviceData';

const SHELLY_ID = 'shellypro2pm-ec6260887e94';
// Comfortably past the shipped probe (20s) and gather (45s) deadlines, so a
// pass means the code recovered rather than that we ran out of patience.
// Lower it via REPRO_PATIENCE_MS when demonstrating the unpatched failure —
// there is nothing to wait for in that case.
const PATIENCE_MS = Number(process.env.REPRO_PATIENCE_MS ?? 60_000);

/**
 * Stand-in for a `ws` socket. Only two behaviours matter here: `send()` is a
 * silent no-op once the socket is closed (that is what `ws` does when no send
 * callback is supplied), and `close` fires on teardown.
 */
class FakeWs extends EventEmitter {
    readyState = 1;
    readonly framesOut: {id: number; method: string}[] = [];

    send(data: string): void {
        if (this.readyState !== 1) return;
        this.framesOut.push(JSON.parse(data));
    }

    close(): void {}

    /** Answer one already-sent request by method name. */
    reply(method: string, result: unknown): void {
        const req = this.framesOut.find((f) => f.method === method);
        if (!req) throw new Error(`no pending ${method} to reply to`);
        this.emit(
            'message',
            JSON.stringify({jsonrpc: '2.0', id: req.id, result})
        );
    }

    /** The link drops: ws closes, the transport tears down. */
    die(): void {
        this.readyState = 3;
        this.emit('close');
    }
}

const tick = () => new Promise((resolve) => setImmediate(resolve));
const wait = (ms: number) => new Promise((resolve) => setTimeout(resolve, ms));

/** Elapsed ms once `p` settles, or null if it outlasts PATIENCE_MS. */
async function timeToSettle(p: Promise<unknown>): Promise<number | null> {
    const startedAt = Date.now();
    const stillPending = Symbol('pending');
    const outcome = await Promise.race([
        p.then(
            () => 'resolved',
            () => 'rejected'
        ),
        wait(PATIENCE_MS).then(() => stillPending)
    ]);
    return outcome === stillPending ? null : Date.now() - startedAt;
}

function report(label: string, value: string): void {
    console.log(`${label.padEnd(22, '.')} ${value}`);
}

async function main(): Promise<number> {
    clearGatheredDataForTests();
    let gatherRuns = 0;

    // --- connection 1: the link drops mid-gather --------------------------
    const ws1 = new FakeWs();
    const transport1 = new WebSocketTransport(ws1 as never);
    const gather = gatherDeviceDataOnce(SHELLY_ID, (signal) => {
        gatherRuns++;
        return ShellyDeviceFactory.gatherDeviceData(transport1, signal);
    });
    gather.catch(() => undefined);

    await tick();
    report('probes sent', ws1.framesOut.map((f) => f.method).join(', '));

    ws1.reply('Shelly.GetDeviceInfo', {
        id: SHELLY_ID,
        fw_id: '20260101-000000'
    });
    ws1.reply('Shelly.GetStatus', {});
    ws1.reply('Shelly.GetConfig', {});
    await tick();
    ws1.die();
    report('link dropped', 'mid-gather, ListMethods unanswered');

    const gatherMs = await timeToSettle(gather);
    report(
        'gather settles',
        gatherMs === null
            ? `NO — still pending after ${PATIENCE_MS}ms`
            : `yes, ${gatherMs}ms`
    );
    report(
        'frames on dead sock',
        `${ws1.framesOut.length} (unchanged — nothing left the process)`
    );

    // --- connection 2: the device reconnects ------------------------------
    const ws2 = new FakeWs();
    const transport2 = new WebSocketTransport(ws2 as never);
    const runsBefore = gatherRuns;
    const second = gatherDeviceDataOnce(SHELLY_ID, (signal) => {
        gatherRuns++;
        return ShellyDeviceFactory.gatherDeviceData(transport2, signal);
    });
    second.catch(() => undefined);
    await tick();

    // A fresh gather and a warm-cache hit are both fine. Joining the dead run
    // is not — that is the failure this reproduces.
    const secondMs = await timeToSettle(second);
    report(
        'reconnect',
        secondMs === null
            ? `HANGS — joined the dead run (${PATIENCE_MS}ms)`
            : `resolves in ${secondMs}ms (fresh gather=${gatherRuns > runsBefore}, RPCs sent=${ws2.framesOut.length})`
    );

    // --- the accept path ---------------------------------------------------
    const acceptMs = await timeToSettle(takeGatheredData(SHELLY_ID));
    report(
        'accept returns',
        acceptMs === null ? `NO — hung ${PATIENCE_MS}ms` : `yes, ${acceptMs}ms`
    );

    const healed = gatherMs !== null && secondMs !== null && acceptMs !== null;
    console.log(
        healed
            ? '\nRESULT: recovers unattended — the device registers on reconnect'
            : '\nRESULT: DEADLOCK — reproduces issues #28 / #30'
    );
    return healed ? 0 : 1;
}

// The gather deadline unrefs its timer by design, so a bare script would exit
// before it fires. Hold the loop open for the duration of the run.
const keepAlive = setInterval(() => {}, 1000);
main()
    .then((code) => {
        clearInterval(keepAlive);
        process.exit(code);
    })
    .catch((err) => {
        clearInterval(keepAlive);
        console.error(err);
        process.exit(2);
    });
