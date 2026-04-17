import {unreachable} from '../../../shared/src/asserts.ts';
import type {Row} from '../../../zero-protocol/src/data.ts';
import type {Change} from './change.ts';
import {maybeSplitAndPushEditChange} from './maybe-split-and-push-edit-change.ts';
import type {InputBase, Output} from './operator.ts';
import type {Stream} from './stream.ts';

export function* filterPush(
  change: Change,
  output: Output,
  pusher: InputBase,
  predicate?: (row: Row) => boolean,
): Stream<'yield'> {
  if (!predicate) {
    process.env.IVM_PARITY_TRACE && console.error(`[ivm:branch:filter-push.ts:15:filter-push-no-predicate type=${change.type}]`);
    yield* output.push(change, pusher);
    return;
  }
  switch (change.type) {
    case 'add':
    case 'remove':
      if (predicate(change.node.row)) {
        process.env.IVM_PARITY_TRACE && console.error(`[ivm:branch:filter-push.ts:21:filter-pass type=${change.type}]`);
        yield* output.push(change, pusher);
      } else {
        process.env.IVM_PARITY_TRACE && console.error(`[ivm:branch:filter-push.ts:24:filter-drop type=${change.type}]`);
      }
      break;
    case 'child':
      if (predicate(change.node.row)) {
        process.env.IVM_PARITY_TRACE && console.error(`[ivm:branch:filter-push.ts:27:filter-pass type=child]`);
        yield* output.push(change, pusher);
      } else {
        process.env.IVM_PARITY_TRACE && console.error(`[ivm:branch:filter-push.ts:30:filter-drop type=child]`);
      }
      break;
    case 'edit':
      yield* maybeSplitAndPushEditChange(change, predicate, output, pusher);
      break;
    default:
      unreachable(change);
  }
}
