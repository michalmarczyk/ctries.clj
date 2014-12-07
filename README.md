# ctries.clj

A Clojure port of
[the original (Scala) implementation of Ctries](https://github.com/axel22/Ctries)
– concurrently modifiable, lock-free ("global progress guaranteed"),
constant-time snapshotable, HAMT-like maps.

Ctries were introduced in the following two papers:

 * [Prokopec, Bagwell, Odersky, *Cache-Aware Lock-Free Concurrent Hash
   Tries*, EPFL 2011](http://infoscience.epfl.ch/record/166908/files/ctries-techreport.pdf)

 * [Prokopec, Bronson, Bagwell, Odersky, *Concurrent Tries with
   Efficient Non-Blocking Snapshots*, EPFL 2011](http://lampwww.epfl.ch/~prokopec/ctries-snapshot.pdf)

Their design bears some similarity to Clojure's transients in that
they prevent in-place modifications to a tree-based data structure
from affecting instances with which it shares structure by tracking
subtree ownership. The mechanism for accomplishing this in a
concurrent setting involves two versions of restricted "double CAS",
of which one (GCAS) is an independently interesting contribution of
the second Ctries paper.

This library was announced at Clojure eXchange 2014; the presentation
can be viewed here:
[Ephemeral-first data structures](https://skillsmatter.com/skillscasts/6028-ephemeral-first-data-structures).

## Usage

There is one public namespace, `ctries.clj`, with a single public
function called `concurrent-map`:

    (require '[ctries.clj :as ct])

    (ct/concurrent-map)
    ;= #<Ctrie {}>

Ctrie objects created by `concurrent-map` implement the transient API
with a few idiosyncracies:

 1. They may be modified in place; `assoc!`, `dissoc!` and `conj!` are
    guaranteed to return the same instance when given a Ctrie.

 2. They may be concurrently accessed and modified by an arbitrary
    number of threads.

 3. They may be passed to `persistent!` any number of times; each such
    call will produce an independent persistent snapshot.

Additionally, both the mutable Ctries produced by `concurrent-map` and
immutable snapshots thereof returned by `persistent!` support `deref`
(also known as `@`); this operation returns independent *mutable*
snapshots:

    (def x (ct/concurrent-map :foo 1))
    (def y @x)
    (def z @x)

    (assoc! x 1 1)
    (assoc! y 2 2)
    (assoc! z 3 3)

    [x y z]
    ;= [#<Ctrie {1 1, :foo 1}> #<Ctrie {:foo 1, 2 2}> #<Ctrie {3 3, :foo 1}>]

## Releases and dependency information

[ctries.clj releases are available from Clojars.](https://clojars.org/ctries.clj)
Please follow the link to discover the current release number.

[Leiningen](http://leiningen.org/) dependency information:

    [ctries.clj "${version}"]

[Maven](http://maven.apache.org/) dependency information:

    <dependency>
      <groupId>ctries.clj</groupId>
      <artifactId>ctries.clj</artifactId>
      <version>${version}</version>
    </dependency>

[Gradle](http://www.gradle.org/) dependency information:

    compile "ctries.clj:ctries.clj:${version}"

## Developer information

Please note that patches will only be accepted from developers who
have submitted the Clojure CA and would be happy with the code they
submit to ctries.clj becoming part of the Clojure project.

## Licence

Copyright © 2014 Michał Marczyk

Distributed under the Apache License, Version 2.0.
