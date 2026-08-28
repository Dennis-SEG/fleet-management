// Direct counter import, not the Observability barrel — the barrel would form
// an import cycle through this util.
import {incrementLabeledCounter} from './observability/counters';

// In-flight dedup. Rejections are not cached; next caller retries.
export class SingleFlight<K, V> {
    readonly #inflight = new Map<K, Promise<V>>();
    readonly #label: string;
    #registered = false;

    constructor(label: string) {
        this.#label = label;
    }

    async run(key: K, fetch: () => Promise<V>): Promise<V> {
        // Lazy register: constructor-time would no-op when modules load before setLevel(2).
        if (!this.#registered) {
            this.#registered = true;
            incrementLabeledCounter(
                'singleflight_dedup_hits',
                {label: this.#label},
                0
            );
        }
        const existing = this.#inflight.get(key);
        if (existing) {
            incrementLabeledCounter('singleflight_dedup_hits', {
                label: this.#label
            });
            return existing;
        }
        const promise = fetch().finally(() => this.#inflight.delete(key));
        this.#inflight.set(key, promise);
        return promise;
    }

    /** The in-flight promise for a key, or undefined if none is running. */
    peek(key: K): Promise<V> | undefined {
        return this.#inflight.get(key);
    }

    /**
     * Stop deduping onto the run for `key`. The run itself is untouched — it
     * settles on its own — but the next caller starts a fresh one instead of
     * joining a run whose result is no longer wanted (its socket is gone, its
     * deadline has passed). Without this, one caller giving up leaves the
     * entry behind for every later caller to join.
     */
    forget(key: K): void {
        this.#inflight.delete(key);
    }

    /** Test-reset only: forget every key at once. */
    clear(): void {
        this.#inflight.clear();
    }

    size(): number {
        return this.#inflight.size;
    }
}
