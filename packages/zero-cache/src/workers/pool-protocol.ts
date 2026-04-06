import type {AST} from '../../../zero-protocol/src/ast.ts';
import type {ClientSchema} from '../../../zero-protocol/src/client-schema.ts';
import type {Row} from '../../../zero-protocol/src/data.ts';
import type {LoadedPermissions} from '../auth/load-permissions.ts';
import type {RowChange} from '../services/view-syncer/pipeline-driver.ts';

// Messages from syncer -> pool worker thread

export type InitMsg = {
  type: 'init';
  clientGroupID: string;
  clientSchema: ClientSchema;
};

export type HydrateMsg = {
  type: 'hydrate';
  clientGroupID: string;
  queryID: string;
  transformationHash: string;
  ast: AST;
};

export type AdvanceMsg = {
  type: 'advance';
  clientGroupID: string;
};

export type DestroyQueryMsg = {
  type: 'destroyQuery';
  clientGroupID: string;
  queryID: string;
};

export type ResetMsg = {
  type: 'reset';
  clientGroupID: string;
  clientSchema: ClientSchema;
};

export type GetRowMsg = {
  type: 'getRow';
  clientGroupID: string;
  table: string;
  rowKey: Record<string, unknown>;
};

export type ShutdownMsg = {
  type: 'shutdown';
};

export type PoolWorkerMsg =
  | InitMsg
  | HydrateMsg
  | AdvanceMsg
  | DestroyQueryMsg
  | ResetMsg
  | GetRowMsg
  | ShutdownMsg;

// Messages from pool worker thread -> syncer

export type InitResult = {
  type: 'initResult';
  version: string;
  replicaVersion: string;
  permissions: LoadedPermissions | null;
};

export type HydrationResult = {
  type: 'hydrationResult';
  queryID: string;
  changes: RowChange[];
  hydrationTimeMs: number;
};

export type AdvanceResult = {
  type: 'advanceResult';
  version: string;
  replicaVersion: string;
  numChanges: number;
  changes: RowChange[];
  didReset: boolean;
  metrics: {
    snapshotMs: number;
    collectMs: number;
    diffReadMs: number;
    ivmPushMs: number;
    totalMs: number;
  };
};

export type DestroyQueryResult = {
  type: 'destroyQueryResult';
  queryID: string;
};

export type ResetResult = {
  type: 'resetResult';
};

export type GetRowResult = {
  type: 'getRowResult';
  row: Row | undefined;
};

export type ErrorResult = {
  type: 'error';
  message: string;
  name: string;
};

export type PoolWorkerResult =
  | InitResult
  | HydrationResult
  | AdvanceResult
  | DestroyQueryResult
  | ResetResult
  | GetRowResult
  | ErrorResult;
