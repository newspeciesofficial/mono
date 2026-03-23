import {assert, unreachable} from '../../../shared/src/asserts.ts';
import {binarySearch} from '../../../shared/src/binary-search.ts';
import type {CompoundKey, System} from '../../../zero-protocol/src/ast.ts';
import type {Value} from '../../../zero-protocol/src/data.ts';
import type {Change} from './change.ts';
import {constraintsAreCompatible} from './constraint.ts';
import type {Node} from './data.ts';
import {
  buildJoinConstraint,
  generateWithOverlayNoYield,
  isJoinMatch,
  rowEqualsForCompoundKey,
  type JoinChangeOverlay,
} from './join-utils.ts';
import {
  throwOutput,
  type FetchRequest,
  type Input,
  type Output,
} from './operator.ts';
import type {SourceSchema} from './schema.ts';
import {type Stream} from './stream.ts';

type Args = {
  parent: Input;
  child: Input;
  // The nth key in childKey corresponds to the nth key in parentKey.
  parentKey: CompoundKey;
  childKey: CompoundKey;

  relationshipName: string;
  hidden: boolean;
  system: System;
};

/**
 * An *inner* join which fetches nodes from its child input first and then
 * fetches their related nodes from its parent input.  Output nodes are the
 * nodes from parent input (in parent input order), which have at least one
 * related child.  These output nodes have a new relationship added to them,
 * which has the name `relationshipName`. The value of the relationship is a
 * stream of related nodes from the child input (in child input order).
 */
export class FlippedJoin implements Input {
  readonly #parent: Input;
  readonly #child: Input;
  readonly #parentKey: CompoundKey;
  readonly #childKey: CompoundKey;
  readonly #relationshipName: string;
  readonly #schema: SourceSchema;

  #output: Output = throwOutput;

  #inprogressChildChange: JoinChangeOverlay | undefined;

  constructor({
    parent,
    child,
    parentKey,
    childKey,
    relationshipName,
    hidden,
    system,
  }: Args) {
    assert(parent !== child, 'Parent and child must be different operators');
    assert(
      parentKey.length === childKey.length,
      'The parentKey and childKey keys must have same length',
    );
    this.#parent = parent;
    this.#child = child;
    this.#parentKey = parentKey;
    this.#childKey = childKey;
    this.#relationshipName = relationshipName;

    const parentSchema = parent.getSchema();
    const childSchema = child.getSchema();
    this.#schema = {
      ...parentSchema,
      relationships: {
        ...parentSchema.relationships,
        [relationshipName]: {
          ...childSchema,
          isHidden: hidden,
          system,
        },
      },
    };

    parent.setOutput({
      push: (change: Change) => this.#pushParent(change),
    });
    child.setOutput({
      push: (change: Change) => this.#pushChild(change),
    });
  }

  destroy(): void {
    this.#child.destroy();
    this.#parent.destroy();
  }

  setOutput(output: Output): void {
    this.#output = output;
  }

  getSchema(): SourceSchema {
    return this.#schema;
  }

  // TODO: When parentKey is the parent's primary key (or more
  // generally when the parent cardinality is expected to be small) a different
  // algorithm should be used:  For each child node, fetch all parent nodes
  // eagerly and then sort using quicksort.
  *fetch(req: FetchRequest): Stream<Node | 'yield'> {
    // Translate constraints for the parent on parts of the join key to
    // constraints for the child.
    const childConstraint: Record<string, Value> = {};
    let hasChildConstraint = false;
    if (req.constraint) {
      for (const [key, value] of Object.entries(req.constraint)) {
        const index = this.#parentKey.indexOf(key);
        if (index !== -1) {
          hasChildConstraint = true;
          childConstraint[this.#childKey[index]] = value;
        }
      }
    }

    const childNodes: Node[] = [];
    for (const node of this.#child.fetch(
      hasChildConstraint ? {constraint: childConstraint} : {},
    )) {
      if (node === 'yield') {
        yield node;
        continue;
      }
      childNodes.push(node);
    }

    // FlippedJoin's split-push change overlay logic is largely
    // the same as Join's with the exception of remove.  For remove,
    // the change is undone here, and then re-applied to parents with order
    // less than or equal to change.position below.  This is necessary
    // because if the removed node was the last related child, the
    // related parents with position greater than change.position
    // (which should not yet have the node removed), would not even
    // be fetched here, and would be absent from the output all together.
    if (this.#inprogressChildChange?.change.type === 'remove') {
      const removedNode = this.#inprogressChildChange.change.node;
      const compare = this.#child.getSchema().compareRows;
      const insertPos = binarySearch(childNodes.length, i =>
        compare(removedNode.row, childNodes[i].row),
      );
      childNodes.splice(insertPos, 0, removedNode);
    }
    // Eagerly fetch parents one child at a time, grouping by parent PK.
    // Each parent iterator is fully consumed before the next starts,
    // so only 1 parent statement is active at a time → full cache reuse.
    const pk = this.#parent.getSchema().primaryKey;
    type CollectedParent = {parent: Node; childIndexes: number[]};
    const parentsByKey = new Map<string, CollectedParent>();

    for (let childIdx = 0; childIdx < childNodes.length; childIdx++) {
      const childNode = childNodes[childIdx];
      const constraintFromChild = buildJoinConstraint(
        childNode.row,
        this.#childKey,
        this.#parentKey,
      );
      if (
        !constraintFromChild ||
        (req.constraint &&
          !constraintsAreCompatible(constraintFromChild, req.constraint))
      ) {
        continue;
      }

      for (const parentNode of this.#parent.fetch({
        ...req,
        constraint: {...req.constraint, ...constraintFromChild},
      })) {
        if (parentNode === 'yield') {
          yield 'yield';
          continue;
        }
        const key = JSON.stringify(pk.map(k => parentNode.row[k]));
        const existing = parentsByKey.get(key);
        if (existing) {
          existing.childIndexes.push(childIdx);
        } else {
          parentsByKey.set(key, {parent: parentNode, childIndexes: [childIdx]});
        }
      }
    }

    // Sort collected parents in output order.
    const sortedParents = [...parentsByKey.values()];
    const compare = this.#schema.compareRows;
    sortedParents.sort(
      (a, b) => compare(a.parent.row, b.parent.row) * (req.reverse ? -1 : 1),
    );

    // Yield sorted parents with overlay logic.
    for (const {parent: minParentNode, childIndexes} of sortedParents) {
      const relatedChildNodes: Node[] = childIndexes.map(i => childNodes[i]);

      let overlaidRelatedChildNodes = relatedChildNodes;
      if (
        this.#inprogressChildChange &&
        this.#inprogressChildChange.position &&
        isJoinMatch(
          this.#inprogressChildChange.change.node.row,
          this.#childKey,
          minParentNode.row,
          this.#parentKey,
        )
      ) {
        const hasInprogressChildChangeBeenPushedForMinParentNode =
          this.#parent
            .getSchema()
            .compareRows(
              minParentNode.row,
              this.#inprogressChildChange.position,
            ) <= 0;
        if (this.#inprogressChildChange.change.type === 'remove') {
          if (hasInprogressChildChangeBeenPushedForMinParentNode) {
            // Remove from relatedChildNodes since the removed child
            // was inserted into childNodes above.
            overlaidRelatedChildNodes = relatedChildNodes.filter(
              n => n !== this.#inprogressChildChange?.change.node,
            );
          }
        } else if (!hasInprogressChildChangeBeenPushedForMinParentNode) {
          overlaidRelatedChildNodes = [
            ...generateWithOverlayNoYield(
              relatedChildNodes,
              this.#inprogressChildChange.change,
              this.#child.getSchema(),
            ),
          ];
        }
      }

      // yield node if after the overlay it still has relationship nodes
      if (overlaidRelatedChildNodes.length > 0) {
        yield {
          ...minParentNode,
          relationships: {
            ...minParentNode.relationships,
            [this.#relationshipName]: () => overlaidRelatedChildNodes,
          },
        };
      }
    }
  }

  *#pushChild(change: Change): Stream<'yield'> {
    switch (change.type) {
      case 'add':
      case 'remove':
        yield* this.#pushChildChange(change);
        break;
      case 'edit': {
        assert(
          rowEqualsForCompoundKey(
            change.oldNode.row,
            change.node.row,
            this.#childKey,
          ),
          `Child edit must not change relationship.`,
        );
        yield* this.#pushChildChange(change, true);
        break;
      }
      case 'child':
        yield* this.#pushChildChange(change, true);
        break;
    }
  }

  *#pushChildChange(change: Change, exists?: boolean): Stream<'yield'> {
    this.#inprogressChildChange = {
      change,
      position: undefined,
    };
    try {
      const constraint = buildJoinConstraint(
        change.node.row,
        this.#childKey,
        this.#parentKey,
      );
      const parentNodeStream = constraint
        ? this.#parent.fetch({constraint})
        : [];
      for (const parentNode of parentNodeStream) {
        if (parentNode === 'yield') {
          yield 'yield';
          continue;
        }
        this.#inprogressChildChange = {
          change,
          position: parentNode.row,
        };
        const childNodeStream = () => {
          const constraint = buildJoinConstraint(
            parentNode.row,
            this.#parentKey,
            this.#childKey,
          );
          return constraint ? this.#child.fetch({constraint}) : [];
        };
        if (!exists) {
          for (const childNode of childNodeStream()) {
            if (childNode === 'yield') {
              yield 'yield';
              continue;
            }
            if (
              this.#child
                .getSchema()
                .compareRows(childNode.row, change.node.row) !== 0
            ) {
              exists = true;
              break;
            }
          }
        }
        if (exists) {
          yield* this.#output.push(
            {
              type: 'child',
              node: {
                ...parentNode,
                relationships: {
                  ...parentNode.relationships,
                  [this.#relationshipName]: childNodeStream,
                },
              },
              child: {
                relationshipName: this.#relationshipName,
                change,
              },
            },
            this,
          );
        } else {
          yield* this.#output.push(
            {
              ...change,
              node: {
                ...parentNode,
                relationships: {
                  ...parentNode.relationships,
                  [this.#relationshipName]: () => [change.node],
                },
              },
            },
            this,
          );
        }
      }
    } finally {
      this.#inprogressChildChange = undefined;
    }
  }

  *#pushParent(change: Change): Stream<'yield'> {
    const childNodeStream = (node: Node) => () => {
      const constraint = buildJoinConstraint(
        node.row,
        this.#parentKey,
        this.#childKey,
      );
      return constraint ? this.#child.fetch({constraint}) : [];
    };

    const flip = (node: Node) => ({
      ...node,
      relationships: {
        ...node.relationships,
        [this.#relationshipName]: childNodeStream(node),
      },
    });

    // If no related child don't push as this is an inner join.
    let hasRelatedChild = false;
    for (const node of childNodeStream(change.node)()) {
      if (node === 'yield') {
        yield 'yield';
        continue;
      } else {
        hasRelatedChild = true;
        break;
      }
    }
    if (!hasRelatedChild) {
      return;
    }

    switch (change.type) {
      case 'add':
      case 'remove':
      case 'child': {
        yield* this.#output.push(
          {
            ...change,
            node: flip(change.node),
          },
          this,
        );
        break;
      }
      case 'edit': {
        assert(
          rowEqualsForCompoundKey(
            change.oldNode.row,
            change.node.row,
            this.#parentKey,
          ),
          `Parent edit must not change relationship.`,
        );
        yield* this.#output.push(
          {
            type: 'edit',
            oldNode: flip(change.oldNode),
            node: flip(change.node),
          },
          this,
        );
        break;
      }
      default:
        unreachable(change);
    }
  }
}
