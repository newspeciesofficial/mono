import type {LogContext} from '@rocicorp/logger';
import {existsSync, readFileSync, writeFileSync} from 'node:fs';
import UrlPattern from 'url-pattern';
import {assert} from '../../../shared/src/asserts.ts';
import {h32} from '../../../shared/src/hash.ts';
import {getOrCreateGauge} from '../observability/metrics.ts';
import {RunningState} from '../services/running-state.ts';
import type {Service} from '../services/service.ts';
import type {IncomingMessageSubset} from '../types/http.ts';
import type {Worker} from '../types/processes.ts';
import {installWebSocketHandoff} from '../types/websocket-handoff.ts';
import {getConnectParams} from '../workers/connect-params.ts';

export class WorkerDispatcher implements Service {
  readonly id = 'worker-dispatcher';
  readonly #lc: LogContext;

  readonly #state = new RunningState(this.id);

  constructor(
    lc: LogContext,
    taskID: string,
    parent: Worker,
    syncers: Worker[],
    mutator: Worker | undefined,
    changeStreamer: Worker | undefined,
    assignmentsFile?: string | undefined,
  ) {
    this.#lc = lc;

    // Least-loaded syncer assignment with persistence.
    // Tracks how many client groups each syncer has and persists
    // the mapping to disk so assignments survive restarts.
    const assignments = new Map<string, number>();
    const syncerLoad = new Array<number>(syncers.length).fill(0);

    // Load persisted assignments if available.
    if (assignmentsFile && existsSync(assignmentsFile)) {
      try {
        const data = JSON.parse(readFileSync(assignmentsFile, 'utf-8'));
        for (const [cg, idx] of Object.entries(data)) {
          const syncerIdx = idx as number;
          if (syncerIdx >= 0 && syncerIdx < syncers.length) {
            assignments.set(cg, syncerIdx);
            syncerLoad[syncerIdx]++;
          }
        }
        lc.info?.(
          `loaded ${assignments.size} syncer assignments from ${assignmentsFile}`,
        );
      } catch (e) {
        lc.warn?.(`failed to load syncer assignments, starting fresh`, e);
      }
    }

    function persistAssignments() {
      if (!assignmentsFile) {
        return;
      }
      try {
        const data = Object.fromEntries(assignments);
        writeFileSync(assignmentsFile, JSON.stringify(data));
      } catch (e) {
        lc.warn?.(`failed to persist syncer assignments`, e);
      }
    }

    function assignSyncer(clientGroupID: string): number {
      if (!assignmentsFile) {
        // Original hash-based routing.
        return h32(taskID + '/' + clientGroupID) % syncers.length;
      }
      const existing = assignments.get(clientGroupID);
      if (existing !== undefined) {
        return existing;
      }
      // Pick the least-loaded syncer.
      let minLoad = syncerLoad[0];
      let minIdx = 0;
      for (let i = 1; i < syncerLoad.length; i++) {
        if (syncerLoad[i] < minLoad) {
          minLoad = syncerLoad[i];
          minIdx = i;
        }
      }
      assignments.set(clientGroupID, minIdx);
      syncerLoad[minIdx]++;
      persistAssignments();
      return minIdx;
    }

    function connectParams(req: IncomingMessageSubset) {
      const {headers, url: u} = req;
      const url = new URL(u ?? '', 'http://unused/');
      const p = parsePath(url);
      if (!p) {
        throw new Error(`Invalid URL: ${u}`);
      }
      const version = Number(p.version);
      if (Number.isNaN(version)) {
        throw new Error(`Invalid version: ${u}`);
      }
      const {params, error} = getConnectParams(version, url, headers);
      if (error !== null) {
        throw new Error(error);
      }
      return params;
    }

    const handlePush = (req: IncomingMessageSubset) => {
      assert(
        mutator !== undefined,
        'Received a push for a custom mutation but no `push.url` was configured.',
      );
      return {payload: connectParams(req), sender: mutator};
    };

    let maxProtocolVersion = 0;
    getOrCreateGauge(
      'sync',
      'max-protocol-version',
      'Latest sync protocol version from a connecting client',
    ).addCallback(result => {
      if (maxProtocolVersion) {
        result.observe(maxProtocolVersion);
      }
    });

    const handleSync = (req: IncomingMessageSubset) => {
      assert(syncers.length, 'Received a sync request with no sync workers.');
      const params = connectParams(req);
      const {clientGroupID, protocolVersion} = params;
      maxProtocolVersion = Math.max(maxProtocolVersion, protocolVersion);

      const syncer = assignSyncer(clientGroupID);

      lc.info?.(`connecting ${clientGroupID} to syncer ${syncer}`);
      return {payload: params, sender: syncers[syncer]};
    };

    const handleChangeStream = (req: IncomingMessageSubset) => {
      // Note: The change-streamer is generally not dispatched via the main
      //       port, and in particular, should *not* be accessible via that
      //       port in single-node mode. However, this plumbing is maintained
      //       for the purpose of allowing --lazy-startup of the
      //       replication-manager as a possible future feature.
      assert(
        syncers.length === 0 && mutator === undefined,
        'Dispatch to the change-streamer via the main port ' +
          'is only allowed in multi-node mode',
      );
      assert(
        changeStreamer,
        'Received a change-streamer request without a change-streamer worker',
      );
      const url = new URL(req.url ?? '', 'http://unused/');
      const path = parsePath(url);
      if (!path) {
        throw new Error(`Invalid URL: ${req.url}`);
      }

      return {
        payload: path.action,
        sender: changeStreamer,
      };
    };

    // handoff messages from this ZeroDispatcher to the appropriate worker (pool).
    installWebSocketHandoff<unknown>(
      lc,
      request => {
        const {url: u} = request;
        const url = new URL(u ?? '', 'http://unused/');
        const path = parsePath(url);
        if (!path) {
          throw new Error(`Invalid URL: ${u}`);
        }
        switch (path.worker) {
          case 'sync':
            return handleSync(request);
          case 'replication':
            return handleChangeStream(request);
          case 'mutate':
            return handlePush(request);
          default:
            throw new Error(`Invalid URL: ${u}`);
        }
      },
      parent,
    );
  }

  run() {
    const readyStart = Date.now();
    getOrCreateGauge('server', 'uptime', {
      description: 'Cumulative uptime, starting from when requests are served',
      unit: 's',
    }).addCallback(result => result.observe((Date.now() - readyStart) / 1000));

    return this.#state.stopped();
  }

  stop() {
    this.#state.stop(this.#lc);
    return this.#state.stopped();
  }
}

const URL_PATTERN = new UrlPattern('(/:base)/:worker/v:version/:action');

export function parsePath(url: URL):
  | {
      base?: string;
      worker: 'sync' | 'mutate' | 'replication';
      version: string;
      action: string;
    }
  | undefined {
  // The match() returns both null and undefined.
  return URL_PATTERN.match(url.pathname) || undefined;
} // The server allows the client to use any /:base/ path to facilitate
// servicing requests on the same domain as the application.
