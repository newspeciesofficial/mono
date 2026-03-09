What's wrong?

CAP fetches from upstream. Upstream is not ordered.

Two places:

1. generateWithOverlay in join
2. merge sort in flippedJoin

Can the comparator be dynamic? The source can tell us the sort it used for the fetch?
How do we know this? By knowing selected index?

If we know that can we just sort in the way sqlite would sort? well our constraints are not static to the connection.
So the sort needs to change by fetch to accommodate new connections.

Source sends the comparator down with the results...

---

What're we going to do to cap?

1. Does merge sort in flipped join need correct ordering if we're feeding to cap? We just need to emit a thing.

Can we figure out the applied ordering after the fact...?
It could be correlated and not causal...

Scan the array to find what sorts it. lmao.

---

Another option... do not apply overlay logic for CAP

1. No overlay for normal join to cap
   What is this `child.position` compare against a parent though? so confusing.
2. flipped-join can infer comparator? Because we will drain the entire child side so we will get the right decision?
