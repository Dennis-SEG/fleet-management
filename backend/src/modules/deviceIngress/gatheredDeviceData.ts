// Holds the device data gathered while a device waits, so accept can assemble
// from it instead of re-fetching. The gather runs at most once per device
// (single-flight — an accept clicked mid-gather shares the in-flight one). A
// failed gather is not kept, so it's re-tried. The entry is dropped when the
// device leaves the waiting room or is accepted.
//
// Every gather is deadline-bound and every wait on one is deadline-bound: a
// gather that can't finish is abandoned rather than left in-flight for the
// next connection to join, so one unresponsive device can't hold its own
// init slot open forever.

import {tuning} from '../../config';
import type {DeviceDataBundle} from '../../model/ShellyDeviceFactory';
import {BoundedMap} from '../boundedMap';
// Direct counter import, not the Observability barrel — the barrel pulls in
// the device model, which imports this module.
import {incrementLabeledCounter} from '../observability/counters';
import {SingleFlight} from '../singleFlight';
import {TimeoutError, withTimeout} from '../util/withTimeout';

const flight = new SingleFlight<string, DeviceDataBundle>('device-gather');
// Bounded + TTL: a gather never consumed (device vanished before accept) is
// evicted rather than pinned. Cleared on accept/leave in the happy path.
const HELD_MAX = 10_000;
const HELD_TTL_MS = 10 * 60 * 1000;
const held = new BoundedMap<string, DeviceDataBundle>({
    maxSize: HELD_MAX,
    ttlMs: HELD_TTL_MS
});
// Per-device generation, bumped every time a running gather is abandoned (an
// accept took it, its deadline passed, the socket went away). A gather that
// finishes late compares the generation it started in against the current one
// and, if they differ, hands its bundle to its own caller without re-storing
// it — a bundle read over a connection that no longer exists must not be
// served to the next accept.
const generation = new BoundedMap<string, number>({
    maxSize: HELD_MAX,
    ttlMs: HELD_TTL_MS
});

function currentGeneration(shellyID: string): number {
    return generation.get(shellyID) ?? 0;
}

// Abandon whatever gather is running for this device: later callers start a
// fresh one, and the abandoned run's result is not stored when it lands.
function abandonGather(shellyID: string, reason: string): void {
    generation.set(shellyID, currentGeneration(shellyID) + 1);
    if (!flight.peek(shellyID)) return;
    flight.forget(shellyID);
    incrementLabeledCounter('device_gather_abandoned_total', {reason});
}

export interface TakeGatheredOptions {
    /** Overrides tuning.waitingRoom.gatherTakeMs. */
    timeoutMs?: number;
    /** The init slot's reclaim signal — abandons the wait when it fires. */
    signal?: AbortSignal;
}

// Gather once and keep the result; a saved bundle or an in-flight gather is
// reused. A rejection is not kept, so the next call re-gathers. The gather is
// deadline-bound: `gather` receives a signal that fires at
// tuning.waitingRoom.gatherMaxMs and must pass it to its RPCs.
export async function gatherDeviceDataOnce(
    shellyID: string,
    gather: (signal: AbortSignal) => Promise<DeviceDataBundle>
): Promise<DeviceDataBundle> {
    const saved = held.get(shellyID);
    if (saved) return saved;
    const startedGeneration = currentGeneration(shellyID);
    const bundle = await flight.run(shellyID, () =>
        withTimeout(
            gather,
            tuning.waitingRoom.gatherMaxMs,
            `device-gather ${shellyID}`
        )
    );
    // Abandoned while we gathered — don't re-store a result nobody wants.
    if (currentGeneration(shellyID) !== startedGeneration) return bundle;
    held.set(shellyID, bundle);
    return bundle;
}

// Remove and return the bundle for accept. If the gather is still running
// (accept clicked mid-gather), await it instead of missing it and re-probing —
// but only up to `gatherTakeMs`, and only until the init slot is reclaimed.
// Giving up returns undefined, so accept falls back to a fresh probe over its
// own socket rather than blocking on a gather that may never land.
export async function takeGatheredData(
    shellyID: string,
    opts: TakeGatheredOptions = {}
): Promise<DeviceDataBundle | undefined> {
    const saved = held.get(shellyID);
    if (saved) {
        held.delete(shellyID);
        return saved;
    }
    const inflight = flight.peek(shellyID);
    if (!inflight) return undefined;
    // Either way this run's result is spoken for: it goes to this accept, or
    // it is given up on. It must not also be stored for the next one.
    generation.set(shellyID, currentGeneration(shellyID) + 1);
    try {
        return await raceTake(
            inflight,
            opts.timeoutMs ?? tuning.waitingRoom.gatherTakeMs,
            opts.signal
        );
    } catch (err) {
        // Timed out, reclaimed, or the gather failed — a run we're no longer
        // waiting on must not be handed to the next connection either.
        abandonGather(shellyID, takeFailureReason(err));
        return undefined;
    }
}

function takeFailureReason(err: unknown): string {
    if (err instanceof TimeoutError) return 'take_timeout';
    if (err instanceof Error && err.name === 'AbortError') return 'reclaimed';
    return 'gather_failed';
}

// Bounded wait on a promise we don't own. `inflight` can't be cancelled from
// here — it belongs to the gather — so this only stops waiting on it; the
// caller abandons the run.
function raceTake(
    inflight: Promise<DeviceDataBundle>,
    timeoutMs: number,
    signal?: AbortSignal
): Promise<DeviceDataBundle> {
    return withTimeout(
        (timeoutSignal) => {
            if (!signal) return inflight;
            return Promise.race([
                inflight,
                abortRejection(AbortSignal.any([signal, timeoutSignal]))
            ]);
        },
        timeoutMs,
        'gather-take'
    );
}

function abortRejection(signal: AbortSignal): Promise<never> {
    return new Promise<never>((_, reject) => {
        if (signal.aborted) {
            reject(abortError(signal));
            return;
        }
        signal.addEventListener('abort', () => reject(abortError(signal)), {
            once: true
        });
    });
}

function abortError(signal: AbortSignal): Error {
    const reason = signal.reason;
    if (reason instanceof Error) return reason;
    const err = new Error(String(reason ?? 'aborted'));
    err.name = 'AbortError';
    return err;
}

export function dropGatheredData(shellyID: string): void {
    held.delete(shellyID);
    // The socket this gather reads over is gone. Drop the in-flight entry too,
    // or the device's next connection dedups onto a run bound to a dead
    // transport and waits out the init-slot watchdog for nothing.
    abandonGather(shellyID, 'socket_closed');
}

export function hasGatheredData(shellyID: string): boolean {
    return held.has(shellyID);
}

export function clearGatheredDataForTests(): void {
    held.clear();
    generation.clear();
    // Also drop in-flight runs — a hung gather left over from one case would
    // otherwise be dedup'd onto by the next and sit out the whole deadline.
    flight.clear();
}
