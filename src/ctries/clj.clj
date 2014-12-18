(ns ctries.clj
  (:refer-clojure :exclude [hash])
  (:import (java.util.concurrent.atomic AtomicReferenceFieldUpdater)
           (java.util Iterator)
           (clojure.lang IPersistentMap MapEntry RT Util APersistentMap)))

(set! *warn-on-reflection* true)
(set! *unchecked-math* :warn-on-boxed)

(defmacro ^:private hash [x]
  `(Util/hasheq ~x))

(deftype RESTART    [])
(deftype NOTFOUND   [])
(deftype KEYABSENT  [])
(deftype KEYPRESENT [])

(deftype Gen [])

;; makeFooUpdater: getting around access restrictions and the fact
;; that there's no way to attach static fields or methods to Clojure
;; types

(gen-interface
  :name ctries.clj.IMaybe
  :methods [])

(gen-interface
  :name ctries.clj.IMainNode
  :methods
  [[makePrevUpdater []
    java.util.concurrent.atomic.AtomicReferenceFieldUpdater]
   [getPrev []                     ctries.clj.IMainNode]
   [setPrev [ctries.clj.IMainNode] void]
   [casPrev [ctries.clj.IMainNode ctries.clj.IMainNode] boolean]])

(gen-interface
  :name ctries.clj.IKVNode
  :methods
  [[copy         [] ctries.clj.IKVNode]
   [copyTombed   [] ctries.clj.IKVNode]
   [copyUntombed [] ctries.clj.IKVNode]
   [kvPair       [] clojure.lang.MapEntry]])

(gen-interface
  :name ctries.clj.IIndirectionNode
  :methods
  [[makeMainNodeUpdater []
    java.util.concurrent.atomic.AtomicReferenceFieldUpdater]
   [makeGenUpdater []
    java.util.concurrent.atomic.AtomicReferenceFieldUpdater]
   [cas [ctries.clj.IMainNode ctries.clj.IMainNode] boolean]
   [getMain []                     ctries.clj.IMainNode]
   [setMain [ctries.clj.IMainNode] void]
   [gen     []                     ctries.clj.Gen]
   [inode   [ctries.clj.IMainNode] ctries.clj.IIndirectionNode]])

(gen-interface
  :name ctries.clj.GCAS
  :methods
  [[gcas
    [ctries.clj.IIndirectionNode
     ctries.clj.IMainNode
     ctries.clj.IMainNode]
    boolean]
   [gcasCommit
    [ctries.clj.IIndirectionNode
     ctries.clj.IMainNode]
    ctries.clj.IMainNode]
   [gcasRead [ctries.clj.IIndirectionNode] ctries.clj.IMainNode]
   [copyToGen
    [ctries.clj.IIndirectionNode
     ctries.clj.Gen]
    ctries.clj.IIndirectionNode]])

(gen-interface
  :name ctries.clj.IBitmapIndexedNode
  :methods
  [[contract   [int]                            ctries.clj.IMainNode]
   [renewed    [ctries.clj.Gen ctries.clj.GCAS] ctries.clj.IBitmapIndexedNode]
   [updatedAt  [int Object ctries.clj.Gen]      ctries.clj.IBitmapIndexedNode]
   [removedAt  [int int ctries.clj.Gen]         ctries.clj.IBitmapIndexedNode]
   [insertedAt [int int Object ctries.clj.Gen]  ctries.clj.IBitmapIndexedNode]
   [dual
    [java.util.Map$Entry
     int
     java.util.Map$Entry
     int
     int
     ctries.clj.Gen]
    ctries.clj.IMainNode]])

(gen-interface
  :name ctries.clj.IHashCollisionNode
  :methods
  [[inserted [Object Object] ctries.clj.IHashCollisionNode]
   [removed  [Object]        Object]
   [get      [Object]        ctries.clj.IMaybe]])

(gen-interface
 :name ctries.clj.RDCSSDescriptor
 :methods
 [[ov            [] ctries.clj.IIndirectionNode]
  [ovmain        [] ctries.clj.IMainNode]
  [nv            [] ctries.clj.IIndirectionNode]
  [committed     [] boolean]
  [markCommitted [] void]])

(gen-interface
 :name ctries.clj.RDCSS
 :methods
 [[rdcss
   [ctries.clj.IIndirectionNode
    ctries.clj.IMainNode
    ctries.clj.IIndirectionNode]
   boolean]
  [rdcssRead     [boolean] ctries.clj.IIndirectionNode]
  [rdcssComplete [boolean] ctries.clj.IIndirectionNode]
  [casRoot [Object Object] boolean]])

(gen-interface
  :name ctries.clj.ICtrie
  :extends [java.lang.Iterable
            clojure.lang.ITransientMap
            clojure.lang.Seqable]
  :methods
  [[makeRootUpdater []
    java.util.concurrent.atomic.AtomicReferenceFieldUpdater]
   [recInsert
    [ctries.clj.IIndirectionNode
     Object
     Object
     int
     int
     ctries.clj.IIndirectionNode
     ctries.clj.Gen]
    boolean]
   [recInsertIf
    [ctries.clj.IIndirectionNode
     Object
     Object
     int
     Object
     int
     ctries.clj.IIndirectionNode
     ctries.clj.Gen]
    ctries.clj.IMaybe]
   [recLookup
    [ctries.clj.IIndirectionNode
     Object
     int
     int
     ctries.clj.IIndirectionNode
     ctries.clj.Gen]
    Object]
   [recRemove
    [ctries.clj.IIndirectionNode
     Object
     Object
     int
     int
     ctries.clj.IIndirectionNode
     ctries.clj.Gen]
    ctries.clj.IMaybe]
   [insertHash   [Object Object int]        void]
   [insertIfHash [Object Object int Object] ctries.clj.IMaybe]
   [lookupHash   [Object int]               Object]
   [removeHash   [Object Object int]        ctries.clj.IMaybe]
   [clean [ctries.clj.IIndirectionNode int ctries.clj.Gen] void]
   [compress
    [ctries.clj.IBitmapIndexedNode int ctries.clj.Gen]
    ctries.clj.IMainNode]
   [resurrect
    [ctries.clj.IIndirectionNode ctries.clj.IMainNode]
    Object]
   [snapshot         [] ctries.clj.ICtrie]
   [readOnlySnapshot [] ctries.clj.ICtrie]])

(gen-interface
  :name ctries.clj.ICtrieIterator
  :methods
  [[initialize   [] void]
   [advance      [] void]
   [checkSubiter [] void]
   [readin       [ctries.clj.IIndirectionNode] void]])

(import (ctries.clj
          ICtrie GCAS RDCSS RDCSSDescriptor ICtrieIterator
          IIndirectionNode IMainNode IKVNode
          IBitmapIndexedNode IHashCollisionNode
          IMaybe))

(deftype Just [x]
  IMaybe)

(deftype Nothing []
  IMaybe)

(defn ^:private just? [x]
  (instance? Just x))

(defn ^:private nothing? [x]
  (instance? Nothing x))

(defn ^:private from-just [x]
  (.-x ^Just x))

(deftype Descriptor [^ctries.clj.IIndirectionNode ov
                     ^ctries.clj.IMainNode ovmain
                     ^ctries.clj.IIndirectionNode nv
                     ^:volatile-mutable ^boolean committed?]
  RDCSSDescriptor
  (ov            [this] ov)
  (ovmain        [this] ovmain)
  (nv            [this] nv)
  (committed     [this] committed?)
  (markCommitted [this] (set! committed? (boolean true))))

(defmacro ^:private descriptor [ov ovmain nv]
  `(Descriptor. ~ov ~ovmain ~nv false))

(deftype Failed [p]
  IMainNode
  (getPrev [this]
    p)

  (setPrev [this _]
    (throw (UnsupportedOperationException.)))

  (casPrev [this _ _]
    (throw (UnsupportedOperationException.)))

  (makePrevUpdater [this]
    (throw (UnsupportedOperationException.))))

(declare ->TombNode)

(deftype SingletonNode [k v ^int h]
  IKVNode
  (copy [this]
    (SingletonNode. k v h))

  (copyTombed [this]
    (->TombNode k v h nil))

  (copyUntombed [this]
    (SingletonNode. k v h))

  (kvPair [this]
    (MapEntry. k v))

  java.util.Map$Entry
  (getKey [this]
    k)

  (getValue [this]
    v))

(declare
  ^AtomicReferenceFieldUpdater indirection-node-mnode-updater
  ^AtomicReferenceFieldUpdater indirection-node-gen-updater)

(deftype IndirectionNode [^:volatile-mutable ^IMainNode mnode
                          ^:volatile-mutable ^Gen gen]
  IIndirectionNode
  (makeMainNodeUpdater [this]
    (AtomicReferenceFieldUpdater/newUpdater IndirectionNode Object "mnode"))

  (makeGenUpdater [this]
    (AtomicReferenceFieldUpdater/newUpdater IndirectionNode Object "gen"))

  (cas [this o n]
    (.compareAndSet indirection-node-mnode-updater this o n))

  (getMain [this]
    (.get indirection-node-mnode-updater this))

  (setMain [this m]
    (.set indirection-node-mnode-updater this m))

  (gen [this]
    gen)

  (inode [this m]
    (IndirectionNode. m gen)))

(def ^:private ^AtomicReferenceFieldUpdater indirection-node-mnode-updater
  (.makeMainNodeUpdater (IndirectionNode. nil nil)))

(def ^:private ^AtomicReferenceFieldUpdater indirection-node-gen-updater
  (.makeGenUpdater (IndirectionNode. nil nil)))

(declare
  ^AtomicReferenceFieldUpdater bitmap-indexed-node-prev-updater
  ->HashCollisionNode)

(deftype BitmapIndexedNode [^:volatile-mutable ^ctries.clj.IMainNode prev
                            ^int bitmap
                            array
                            ^Gen gen]
  IMainNode
  (getPrev [this]
    (.get bitmap-indexed-node-prev-updater this))

  (setPrev [this x]
    (.set bitmap-indexed-node-prev-updater this x))

  (casPrev [this o n]
    (.compareAndSet bitmap-indexed-node-prev-updater this o n))

  (makePrevUpdater [this]
    (AtomicReferenceFieldUpdater/newUpdater
      BitmapIndexedNode Object "prev"))

  IBitmapIndexedNode
  (contract [this shift]
    (if (and (== 1 (alength ^objects array))
             (pos? shift))
      (let [child (aget ^objects array 0)]
        (if (instance? SingletonNode child)
          (.copyTombed ^SingletonNode child)
          this))
      this))

  (renewed [this ngen ctrie]
    (let [len (alength ^objects array)
          out (object-array len)]
      (dotimes [i len]
        (let [child (aget ^objects array i)]
          (if (instance? IndirectionNode child)
            (aset ^objects out i (.copyToGen ctrie child ngen))
            (aset ^objects out i child))))
      (BitmapIndexedNode. nil bitmap out ngen)))

  (updatedAt [this idx new-child ngen]
    (let [len (alength ^objects array)
          out (object-array len)]
      (System/arraycopy array 0 out 0 len)
      (aset ^objects out idx new-child)
      (BitmapIndexedNode. nil bitmap out ngen)))

  (removedAt [this idx flag ngen]
    (let [len (alength ^objects array)
          out (object-array (dec len))]
      (System/arraycopy array 0 out 0 idx)
      (System/arraycopy array (inc idx) out idx (dec (- len idx)))
      (BitmapIndexedNode. nil (bit-xor bitmap flag) out ngen)))

  (insertedAt [this idx flag new-child ngen]
    (let [len (alength ^objects array)
          out (object-array (inc len))]
      (System/arraycopy array 0 out 0 idx)
      (aset ^objects out idx new-child)
      (System/arraycopy array idx out (inc idx) (- len idx))
      (BitmapIndexedNode. nil (bit-or bitmap flag) out ngen)))

  (dual [this x xh y yh shift gen]
    (if (< shift 35)
      (let [xidx (bit-and (unsigned-bit-shift-right xh shift) 0x1f)
            yidx (bit-and (unsigned-bit-shift-right yh shift) 0x1f)
            bmp  (bit-or (bit-shift-left 1 xidx) (bit-shift-left 1 yidx))]
        (cond
          (== xidx yidx)
          (let [sub (IndirectionNode.
                      (.dual this x xh y yh (+ shift 5) gen) gen)
                arr (object-array 1)]
            (aset ^objects arr 0 sub)
            (BitmapIndexedNode. nil bmp arr gen))

          (< xidx yidx)
          (let [arr (object-array 2)]
            (aset ^objects arr 0 x)
            (aset ^objects arr 1 y)
            (BitmapIndexedNode. nil bmp arr gen))

          :else
          (let [arr (object-array 2)]
            (aset ^objects arr 0 y)
            (aset ^objects arr 1 x)
            (BitmapIndexedNode. nil bmp arr gen))))
      (->HashCollisionNode
        (array-map
          (.getKey x) (.getValue x)
          (.getKey y) (.getValue y))
        nil))))

(def ^:private ^AtomicReferenceFieldUpdater bitmap-indexed-node-prev-updater
  (.makePrevUpdater (BitmapIndexedNode. nil 0 nil nil)))

(defn ^:private new-root-node []
  (let [gen (Gen.)]
    (IndirectionNode. (BitmapIndexedNode. nil 0 (object-array 0) gen) gen)))

(declare ^AtomicReferenceFieldUpdater tomb-node-prev-updater)

(deftype TombNode [k v ^int h
                   ^:volatile-mutable ^IMainNode prev]
  IMainNode
  (getPrev [this]
    (.get tomb-node-prev-updater this))

  (setPrev [this x]
    (.set tomb-node-prev-updater this x))

  (casPrev [this o n]
    (.compareAndSet tomb-node-prev-updater this o n))

  (makePrevUpdater [this]
    (AtomicReferenceFieldUpdater/newUpdater
      TombNode Object "prev"))

  IKVNode
  (copy [this]
    (TombNode. k v h nil))

  (copyTombed [this]
    (TombNode. k v h nil))

  (copyUntombed [this]
    (SingletonNode. k v h))

  (kvPair [this]
    (MapEntry. k v)))

(def ^:private ^AtomicReferenceFieldUpdater tomb-node-prev-updater
  (.makePrevUpdater ^TombNode (->TombNode nil nil -1 nil)))

(declare ^AtomicReferenceFieldUpdater hash-collision-node-prev-updater)

(deftype HashCollisionNode [^IPersistentMap kvs
                            ^:volatile-mutable prev]
  IMainNode
  (getPrev [this]
    (.get hash-collision-node-prev-updater this))

  (setPrev [this x]
    (.set hash-collision-node-prev-updater this x))

  (casPrev [this o n]
    (.compareAndSet hash-collision-node-prev-updater this o n))

  (makePrevUpdater [this]
    (AtomicReferenceFieldUpdater/newUpdater
      HashCollisionNode Object "prev"))

  IHashCollisionNode
  (inserted [this k v]
    (HashCollisionNode. (assoc kvs k v) nil))

  (removed [this k]
    (let [kvs' (dissoc kvs k)]
      (if (> (count kvs') 1)
        (HashCollisionNode. kvs' nil)
        (let [[k v] (first kvs')]
          (TombNode. k v (hash k) nil)))))

  (get [this k]
    (let [v (get kvs k this)]
      (if (identical? v this)
        (Nothing.)
        (Just. v)))))

(def ^:private ^AtomicReferenceFieldUpdater hash-collision-node-prev-updater
  (.makePrevUpdater ^HashCollisionNode (->HashCollisionNode nil nil)))

;;; not what you'd typically describe as pretty
(defmacro ^:private inserted* []
  '(let [m' (.inserted m k v)]
     (.gcas this inode m m')))

(declare ^AtomicReferenceFieldUpdater ctrie-root-updater)

(declare ctrie-iterator ->ImmutableCtrie)

(deftype Ctrie [^:volatile-mutable root ; IIndirectionNode or RDCSSDescriptor
                ^boolean read-only?]

  ICtrie
  (makeRootUpdater [this]
    (AtomicReferenceFieldUpdater/newUpdater
      Ctrie Object "root"))

  (recInsert [this inode k v h shift parent startgen]
    (loop [inode inode parent parent shift shift]
      (let [m (.gcasRead this inode)]
        (cond
          (instance? BitmapIndexedNode m)
          (let [idx  (bit-and (unsigned-bit-shift-right h shift) 0x1f)
                flag (bit-shift-left 1 idx)
                bmp  (.-bitmap ^BitmapIndexedNode m)
                mask (dec flag)
                pos  (Integer/bitCount (bit-and bmp mask))]
            (if (zero? (bit-and bmp flag))
              (let [g  (.gen ^IIndirectionNode inode)
                    rn (if (identical? g (.-gen ^BitmapIndexedNode m))
                         m
                         (.renewed ^BitmapIndexedNode m g this))
                    m' (.insertedAt ^BitmapIndexedNode rn
                         pos flag (SingletonNode. k v h) g)]
                (.gcas this inode m m'))
              (let [arr   (.-array ^BitmapIndexedNode m)
                    child (aget ^objects arr pos)]
                (cond
                  (instance? IndirectionNode child)
                  (let [g (.gen ^IIndirectionNode child)]
                    (cond
                      (identical? g startgen)
                      (recur child inode (+ shift 5))

                      (.gcas this inode m
                        (.renewed ^BitmapIndexedNode m startgen this))
                      (recur inode parent shift)

                      :else false))

                  (instance? SingletonNode child)
                  (let [g (.gen ^IIndirectionNode inode)]
                    (if (and (== h (.-h ^SingletonNode child))
                             (= k (.-k ^SingletonNode child)))
                      (.gcas this inode m
                        (.updatedAt ^BitmapIndexedNode m
                          pos (SingletonNode. k v h) g))
                      (let [rn (if (identical? g (.-gen ^BitmapIndexedNode m))
                                 m
                                 (.renewed ^BitmapIndexedNode m g this))
                            m' (.updatedAt ^BitmapIndexedNode rn pos
                                 (.inode inode
                                   (.dual ^BitmapIndexedNode rn
                                     child (.-h ^SingletonNode child)
                                     (SingletonNode. k v h) h
                                     (+ shift 5)
                                     g))
                                 g)]
                        (.gcas this inode m m'))))))))

          (instance? TombNode m)
          (do
            (.clean this parent (- shift 5) (.gen ^IIndirectionNode inode))
            false)

          (instance? HashCollisionNode m)
          (let [m' (.inserted ^HashCollisionNode m k v)]
            (.gcas this inode m m'))))))

  (recInsertIf [this inode k v h condition shift parent startgen]
    (loop [inode inode parent parent shift shift]
      (let [m (.gcasRead this inode)]
        (cond
          (instance? BitmapIndexedNode m)
          (let [idx  (bit-and (unsigned-bit-shift-right h shift) 0x1f)
                flag (bit-shift-left 1 idx)
                bmp  (.-bitmap ^BitmapIndexedNode m)
                mask (dec flag)
                pos  (Integer/bitCount (bit-and bmp mask))]
            (if (zero? (bit-and bmp flag))
              (if (or (nil? condition)
                      (identical? condition KEYABSENT))
                (let [g  (.gen ^IIndirectionNode inode)
                      rn (if (identical? (.-gen ^BitmapIndexedNode m) g)
                           m
                           (.renewed ^BitmapIndexedNode m g this))
                      m' (.insertedAt ^BitmapIndexedNode rn
                           pos flag (SingletonNode. k v h) g)]
                  (if (.gcas this inode m m')
                    (Nothing.)
                    nil))
                (Nothing.))
              (let [child (aget ^objects (.-array ^BitmapIndexedNode m) pos)]
                (cond
                  (instance? IndirectionNode child)
                  (let [g (.gen ^IIndirectionNode child)]
                    (cond
                      (identical? g startgen)
                      (recur child inode (+ shift 5))

                      (.gcas this inode m
                        (.renewed ^BitmapIndexedNode m startgen this))
                      (recur inode parent shift)

                      :else nil))

                  (instance? SingletonNode child)
                  (let [child ^SingletonNode child]
                    (condp identical? condition
                      nil
                      (if (and (== (.-h child) h)
                               (= (.-k child) k))
                        (if (.gcas this inode m
                              (.updatedAt ^BitmapIndexedNode m
                                pos (SingletonNode. k v h)
                                (.gen ^IIndirectionNode inode)))
                          (Just. (.-v child))
                          nil)
                        (let [g  (.gen ^IIndirectionNode inode)
                              rn (if (identical? (.-gen ^BitmapIndexedNode m) g)
                                   m
                                   (.renewed ^BitmapIndexedNode m g this))
                              m' (.updatedAt ^BitmapIndexedNode rn
                                   pos
                                   (.inode inode
                                     (.dual ^BitmapIndexedNode rn
                                       child (.-h child)
                                       (SingletonNode. k v h) h
                                       (+ shift 5)
                                       g))
                                   g)]
                          (if (.gcas this inode m m')
                            (Nothing.)
                            nil)))

                      KEYABSENT
                      (if (and (== (.-h child) h)
                               (= (.-k child) k))
                        (Just. (.-v child))
                        (let [g  (.gen ^IIndirectionNode inode)
                              rn (if (identical? (.-gen ^BitmapIndexedNode m) g)
                                   m
                                   (.renewed ^BitmapIndexedNode m g this))
                              m' (.updatedAt ^BitmapIndexedNode rn
                                   pos
                                   (.inode inode
                                     (.dual ^BitmapIndexedNode rn
                                       child (.-h child)
                                       (SingletonNode. k v h) h
                                       (+ shift 5)
                                       g))
                                   g)]
                          (if (.gcas this inode m m')
                            (Nothing.)
                            nil)))

                      KEYPRESENT
                      (if (and (== (.-h child) h)
                               (= (.-k child) k))
                        (if (.gcas this inode m
                              (.updatedAt ^BitmapIndexedNode m
                                pos (SingletonNode. k v h)
                                (.gen ^IIndirectionNode inode)))
                          (Just. (.-v child))
                          nil)
                        (Nothing.))

                      (if (and (== (.-h child) h)
                               (= (.-k child) k)
                               (= (.-v child) condition))
                        (if (.gcas this inode m
                              (.updatedAt ^BitmapIndexedNode m
                                pos (SingletonNode. k v h)
                                (.gen ^IIndirectionNode inode)))
                          (Just. (.-v child))
                          nil)
                        (Nothing.))))))))

          (instance? TombNode m)
          (do
            (.clean this parent (- shift 5) (.gen ^IIndirectionNode inode))
            nil)

          (instance? HashCollisionNode m)
          (let [m ^HashCollisionNode m
                b (.get m k)]
            (condp identical? condition
              nil
              (if (inserted*)
                b
                nil)

              KEYABSENT
              (if (nil? b)
                (if (inserted*)
                  (Nothing.)
                  nil)
                b)

              KEYPRESENT
              (cond
                (nil? b)    nil
                (inserted*) b
                :else       nil)

              (let [v condition]
                (if (and (just? b) (= v (from-just b)))
                  (if (inserted*)
                    b
                    nil)
                  nil))))))))

  (recLookup [this inode k h shift parent startgen]
    (loop [inode inode parent parent shift shift]
      (let [m (.gcasRead this inode)]
        (cond
          (instance? BitmapIndexedNode m)
          (let [idx  (bit-and (unsigned-bit-shift-right h shift) 0x1f)
                flag (bit-shift-left 1 idx)
                bmp  (.-bitmap ^BitmapIndexedNode m)]
            (if (zero? (bit-and bmp flag))
              NOTFOUND
              (let [pos (if (== bmp 0xffffffff)
                          idx
                          (Integer/bitCount (bit-and bmp (dec flag))))
                    sub (aget ^objects (.-array ^BitmapIndexedNode m) pos)]
                (cond
                  (instance? IndirectionNode sub)
                  (cond
                    (or read-only?
                        (identical? startgen (.gen ^IIndirectionNode sub)))
                    (recur sub inode (+ shift 5))

                    (.gcas this inode m
                      (.renewed ^BitmapIndexedNode m startgen this))
                    (recur inode parent shift)

                    :else RESTART)

                  (instance? SingletonNode sub)
                  (let [sub ^SingletonNode sub]
                    (if (and (== (.-h sub) h)
                             (= (.-k sub) k))
                      (.-v sub)
                      NOTFOUND))))))

          (instance? TombNode m)
          (if read-only?
            (let [m ^TombNode m]
              (if (and (== (.-h m) h)
                       (= (.-k m) k))
                (.-v m)
                NOTFOUND))
            (do
              (.clean this parent (- shift 5) (.gen ^IIndirectionNode inode))
              RESTART))

          (instance? HashCollisionNode m)
          (let [b (.get ^HashCollisionNode m k)]
            (if (just? b)
              (from-just b)
              NOTFOUND))))))

  (recRemove [this inode k v h shift parent startgen]
    (let [m (.gcasRead this inode)]
      (cond
        (instance? BitmapIndexedNode m)
        (let [idx  (bit-and (unsigned-bit-shift-right h shift) 0x1f)
              bmp  (.-bitmap ^BitmapIndexedNode m)
              flag (bit-shift-left 1 idx)]
          (if (zero? (bit-and bmp flag))
            (Nothing.)
            (let [pos (Integer/bitCount (bit-and bmp (dec flag)))
                  sub (aget ^objects (.-array ^BitmapIndexedNode m) pos)
                  ret (cond
                        (instance? IndirectionNode sub)
                        (let [g (.gen ^IIndirectionNode sub)]
                          (cond
                            (identical? g startgen)
                            (.recRemove this
                              sub k v h (+ shift 5) inode startgen)

                            (.gcas this inode m
                              (.renewed ^BitmapIndexedNode m startgen this))
                            (.recRemove this
                              inode k v h shift parent startgen)

                            :else nil))

                        (instance? SingletonNode sub)
                        (let [sub ^SingletonNode sub]
                          (if (and (== (.-h sub) h)
                                   (= (.-k sub) k)
                                   (or (nil? v) (= (.-v sub) v)))
                            (let [nc (.contract
                                       (.removedAt ^BitmapIndexedNode m
                                         pos flag
                                         (.gen ^IIndirectionNode inode))
                                       shift)]
                              (if (.gcas this inode m nc)
                                (Just. (.-v sub))
                                nil))
                            (Nothing.))))]
              (cond
                (or (nil? ret) (nothing? ret))
                ret

                (not (nil? parent))
                (let [non-live (.gcasRead this inode)]
                  (when (instance? TombNode non-live)
                    (loop []
                      (let [pm (.gcasRead this parent)]
                        (if (instance? BitmapIndexedNode pm)
                          (let [pm   ^BitmapIndexedNode pm
                                idx  (bit-and
                                       (unsigned-bit-shift-right
                                         h (- shift 5))
                                       0x1f)
                                bmp  (.-bitmap pm)
                                flag (bit-shift-left 1 idx)]
                            (if-not (zero? (bit-and bmp flag))
                              (let [pos (Integer/bitCount
                                          (bit-and bmp (dec flag)))
                                    sub (aget ^objects (.-array pm) pos)]
                                (if (identical? sub this)
                                  (if (instance? TombNode non-live)
                                    (let [m  ^BitmapIndexedNode m
                                          nc (.updatedAt m pos
                                               (.copyUntombed
                                                 ^TombNode non-live)
                                               (.gen ^IIndirectionNode inode))
                                          nc (.contract nc (- shift 5))]
                                      (if-not (.gcas this parent pm nc)
                                        (let [r (.rdcssRead this false)
                                              g (.gen ^IIndirectionNode r)]
                                          (if (identical? g startgen)
                                            (recur))))))))))))))
                  ret)

                :else ret))))

        (instance? TombNode m)
        (do
          (.clean this parent (- shift 5) (.gen ^IIndirectionNode inode))
          nil)

        (instance? HashCollisionNode m)
        (let [b (.get ^HashCollisionNode m k)]
          (if (or (nil? v)
                  (and (just? b) (= v (from-just b))))
            (let [m' (.removed ^HashCollisionNode m k)]
              (if (.gcas this inode m m')
                b
                nil))
            (Nothing.))))))

  (insertHash [this k v h]
    (loop []
      (let [r (.rdcssRead this false)]
        (if-not (.recInsert this r k v h 0 nil (.gen ^IIndirectionNode r))
          (recur)))))

  (insertIfHash [this k v h condition]
    (loop []
      (let [r   (.rdcssRead this false)
            g   (.gen ^IIndirectionNode r)
            ret (.recInsertIf this r k v h condition 0 nil g)]
        (if (nil? ret)
          (recur)
          ret))))

  (lookupHash [this k h]
    (loop []
      (let [r   (.rdcssRead this false)
            g   (.gen ^IIndirectionNode r)
            ret (.recLookup this r k h 0 nil g)]
        (if (identical? ret RESTART)
          (recur)
          ret))))

  (removeHash [this k v h]
    (loop []
      (let [r   (.rdcssRead this false)
            g   (.gen ^IIndirectionNode r)
            ret (.recRemove this r k v h 0 nil g)]
        (if (nil? ret)
          (recur)
          ret))))

  (snapshot [this]
    (loop []
      (let [r (.rdcssRead this false)
            m (.gcasRead this r)]
        (if (.rdcss this r m (.copyToGen this r (Gen.)))
          (Ctrie. (.copyToGen this r (Gen.)) false)
          (recur)))))

  (readOnlySnapshot [this]
    (loop []
      (let [r (.rdcssRead this false)
            m (.gcasRead this r)]
        (if (.rdcss this r m (.copyToGen this r (Gen.)))
          (Ctrie. r true)
          (recur)))))

  (clean [this inode shift gen]
    (let [m (.gcasRead this inode)]
      (if (instance? BitmapIndexedNode m)
        (.gcas this inode m
          (.compress this m shift gen)))))

  (compress [this bin shift gen]
    (let [bin ^BitmapIndexedNode bin
          bmp (.-bitmap bin)
          arr (.-array bin)
          len (alength ^objects arr)
          out (object-array len)]
      (dotimes [i len]
        (let [sub (aget ^objects arr i)]
          (cond
            (instance? IndirectionNode sub)
            (let [m (.gcasRead this sub)]
              (assert (not (nil? m)))
              (aset ^objects out i (.resurrect this sub m)))

            (instance? SingletonNode sub)
            (aset ^objects out i sub))))
      (.contract (BitmapIndexedNode. nil bmp out gen) shift)))

  (resurrect [this inode mnode]
    (if (instance? TombNode mnode)
      (.copyUntombed ^TombNode mnode)
      inode))

  GCAS
  (gcas [this inode o n]
    (.setPrev n o)
    (if (.cas inode o n)
      (do
        (.gcasCommit this inode n)
        (nil? (.getPrev n)))
      false))

  (gcasCommit [this inode mnode]
    (loop [mnode mnode]
      (if (nil? mnode)
        nil
        (let [p (.getPrev mnode)
              r (.rdcssRead this true)]
          (cond
            (nil? p) mnode

            (instance? Failed p)
            (let [pp (.getPrev p)]
              (if (.cas inode mnode pp)
                pp
                (recur (.getMain inode))))

            (instance? IMainNode p)
            (if (and (identical?
                       (.gen ^IIndirectionNode r)
                       (.gen ^IIndirectionNode inode))
                     (not read-only?))
              (if (.casPrev mnode p nil)
                mnode
                (recur mnode))
              (do
                (.casPrev mnode p (Failed. p))
                (recur (.getMain inode)))))))))

  (gcasRead [this inode]
    (let [m (.getMain inode)]
      (if (nil? (.getPrev m))
        m
        (.gcasCommit this inode m))))

  (copyToGen [this inode gen]
    (IndirectionNode. (.gcasRead this inode) gen))
  
  RDCSS
  (rdcss [this ov ovmain nv]
    (let [desc (descriptor ov ovmain nv)]
      (if (.casRoot this ov desc)
        (do
          (.rdcssComplete this false)
          (.committed desc))
        false)))

  (rdcssComplete [this abort?]
    (loop []
      (let [r (.get ctrie-root-updater this)]
        (if (instance? IIndirectionNode r)
          r
          (let [r ^RDCSSDescriptor r
                ov  (.ov r)
                exp (.ovmain r)
                nv  (.nv r)]
            (if abort?
              (if (.casRoot this r ov)
                ov
                (recur))
              (let [ovmain (.gcasRead this ov)]
                (if (identical? ovmain exp)
                  (if (.casRoot this r nv)
                    (do
                      (.markCommitted r)
                      nv)
                    (recur))
                  (if (.casRoot this r ov)
                    ov
                    (recur))))))))))

  (rdcssRead [this abort?]
    (let [r (.get ctrie-root-updater this)]
      (if (instance? Descriptor r)
        (.rdcssComplete this abort?)
        r)))

  (casRoot [this o n]
    (.compareAndSet ctrie-root-updater this o n))

  java.util.concurrent.ConcurrentMap
  (putIfAbsent [this k v]
    (let [maybe (.insertIfHash this k v (hash k) KEYABSENT)]
      (if (just? maybe)
        (from-just maybe)
        nil)))

  (remove [this k v]
    (just? (.removeHash this k v (hash k))))

  (replace [this k v]
    (let [maybe (.insertIfHash this k v (hash k) KEYPRESENT)]
      (if (just? maybe)
        (from-just maybe)
        nil)))

  (replace [this k old-v new-v]
    (just? (.insertIfHash this k new-v (hash k) old-v)))

  java.util.Map
  (clear [this]
    (loop []
      (let [r (.rdcssRead this false)]
        (if-not (.rdcss this r (.gcasRead this r) (new-root-node))
          (recur)))))

  (containsKey [this k]
    (not (identical? (.lookupHash this k (hash k)) NOTFOUND)))

  (containsValue [this v]
    (some #(= % v) (.values this)))

  (entrySet [this]
    (let [iter (.. this (readOnlySnapshot) (iterator))]
      (java.util.HashSet. ^clojure.lang.IteratorSeq (iterator-seq iter))))

  (equals [this that]
    (cond
      (identical? this that)               true
      (not (instance? java.util.Map that)) false
      :else
      (let [m ^java.util.Map that]
        (if (== (.size m) (.size this))
          (loop [xs (seq this)]
            (if xs
              (let [e      ^java.util.Map$Entry (first xs)
                    k      (.getKey e)
                    found? (.containsKey m k)]
                (if (or (not found?)
                        (not (Util/equals (.getValue e) (.get m k))))
                  false
                  (recur (next xs))))
              true))
          false))))

  (get [this k]
    (let [result (.lookupHash this k (hash k))]
      (if (identical? result NOTFOUND)
        nil
        result)))

  (isEmpty [this]
    (let [r (.rdcssRead this false)
          m (.gcasRead this r)
          a (.-array ^BitmapIndexedNode m)]
      (zero? (alength ^objects a))))

  (keySet [this]
    (let [iter (.. this (readOnlySnapshot) (iterator))
          ks   ^java.util.Collection (map key (iterator-seq iter))]
      (java.util.HashSet. ks)))

  (put [this k v]
    (let [b (.insertIfHash this k v (hash k) nil)]
      (if (just? b)
        (from-just b)
        nil)))

  (putAll [this m]
    (let [iter (.. m (entrySet) (iterator))]
      (while (.hasNext iter)
        (let [e ^java.util.Map$Entry (.next iter)]
          (.put this (.getKey e) (.getValue e))))))

  (remove [this k]
    (let [ret (.removeHash this k nil (hash k))]
      (if (just? ret)
        (from-just ret)
        nil)))

  (size [this]
    (let [iter (.. this (readOnlySnapshot) (iterator))]
      (loop [i 0]
        (if (.hasNext iter)
          (do
            (.next iter)
            (recur (inc i)))
          i))))

  (values [this]
    (let [iter (.. this (readOnlySnapshot) (iterator))]
      (map val (iterator-seq iter))))

  Iterable
  (iterator [this]
    (ctrie-iterator (if read-only? this (.readOnlySnapshot this))))

  java.io.Serializable

  Object
  (hashCode [this]
    (.hashCode ^Object (->ImmutableCtrie (.readOnlySnapshot this) nil)))

  clojure.lang.IFn
  (invoke [this k]
    (.valAt this k))

  (invoke [this k not-found]
    (.valAt this k not-found))

  (applyTo [this args]
    (let [n (RT/boundedLength args 2)]
      (case n
        0 (throw (clojure.lang.ArityException.
                  n (.. this (getClass) (getSimpleName))))
        1 (.invoke this (first args))
        2 (.invoke this (first args) (second args))
        3 (throw (clojure.lang.ArityException.
                  n (.. this (getClass) (getSimpleName)))))))

  clojure.lang.ITransientMap
  (count [this]
    (.size this))

  (valAt [this k]
    (.valAt this k nil))

  (valAt [this k not-found]
    (let [result (.lookupHash this k (hash k))]
      (if (identical? result NOTFOUND)
        not-found
        result)))

  (conj [this o]
    (cond
      (instance? java.util.Map$Entry o)
      (let [e ^java.util.Map$Entry o]
        (.put this (.getKey e) (.getValue e)))

      (instance? clojure.lang.IPersistentVector o)
      (let [v ^clojure.lang.IPersistentVector o]
        (if-not (== (.count v) 2)
          (throw
            (IllegalArgumentException.
              "Vector arg to map conj must be a pair"))
          (.put this (.nth v 0) (.nth v 1))))

      ;; assuming it's seqable
      :else
      (doseq [^java.util.Map$Entry e (seq o)]
        (.put this (.getKey e) (.getValue e))))
    this)

  (assoc [this k v]
    (.insertHash this k v (hash k))
    this)

  (without [this k]
    (.remove this k)
    this)

  (persistent [this]
    (->ImmutableCtrie (.readOnlySnapshot this) nil))

  clojure.lang.Seqable
  (seq [this]
    (iterator-seq (.iterator this)))

  clojure.lang.IDeref
  (deref [this]
    (.snapshot this))

  clojure.lang.IHashEq
  (hasheq [this]
    (hash-unordered-coll this)))

(def ^:private ^AtomicReferenceFieldUpdater ctrie-root-updater
  (.makeRootUpdater (Ctrie. nil true)))

(def ^:private map-print-method
  (get-method print-method java.util.Map))

(defmethod print-method ctries.clj.Ctrie [^Ctrie ctrie ^java.io.Writer w]
  (.write w "#<Ctrie ")
  (map-print-method ctrie w)
  (.write w ">"))

(deftype CtrieIterator [^Ctrie ctrie
                        ^objects stack
                        ^ints stackpos
                        ^:unsynchronized-mutable ^int depth
                        ^:unsynchronized-mutable ^Iterator subiter
                        ^:unsynchronized-mutable ^IKVNode current]
  Iterator
  (hasNext [this]
    (or (boolean current) (boolean subiter)))

  (next [this]
    (if (.hasNext this)
      (if (nil? subiter)
        (let [ret (.kvPair current)]
          (.advance this)
          ret)
        (let [ret (.next subiter)]
          (.checkSubiter this)
          ret))
      (throw (java.util.NoSuchElementException.))))

  ICtrieIterator
  (initialize [this]
    (assert (.-read-only? ctrie))
    (let [r (.rdcssRead ctrie false)]
      (.readin this r)))

  (readin [this inode]
    (let [m (.gcasRead ctrie inode)]
      (cond
        (instance? BitmapIndexedNode m)
        (let [d (unchecked-inc-int depth)]
          (set! depth d)
          (aset stack d (.-array ^BitmapIndexedNode m))
          (aset stackpos d -1)
          (.advance this))

        (instance? TombNode m)
        (set! current ^TombNode m)
        
        (instance? HashCollisionNode m)
        (let [iter (.iterator ^IPersistentMap (.-kvs ^HashCollisionNode m))]
          (set! subiter iter)
          (.checkSubiter this))

        (nil? m)
        (set! current nil))))

  (checkSubiter [this]
    (when-not (.hasNext subiter)
      (set! subiter nil)
      (.advance this)))

  (advance [this]
    (if (neg? depth)
      (set! current nil)
      (let [npos (unchecked-inc-int (aget stackpos depth))]
        (if (< npos (alength ^objects (aget stack depth)))
          (let [n (aget ^objects (aget stack depth) npos)]
            (aset stackpos depth npos)
            (cond
              (instance? SingletonNode n)
              (set! current n)

              (instance? IndirectionNode n)
              (.readin this n)))
          (do
            (set! depth (unchecked-dec-int depth))
            (.advance this)))))))

(defn ^:private ctrie-iterator
  ([ctrie]
     (ctrie-iterator ctrie true))
  ([ctrie must-init?]
     (let [stack    (into-array Object
                      (repeatedly 7 #(make-array Object 7)))
           stackpos (int-array 7)
           iter     (CtrieIterator. ctrie stack stackpos -1 nil nil)]
       (if must-init?
         (.initialize iter))
       iter)))

(declare concurrent-map)

(deftype ImmutableCtrie [^Ctrie ctrie _meta]
  clojure.lang.MapEquivalence

  clojure.lang.IPersistentMap
  (count [this]
    (.count ctrie))

  (empty [this]
    (ImmutableCtrie. (concurrent-map) _meta))

  (cons [this o]
    (ImmutableCtrie. (.conj (.snapshot ctrie) o) _meta))

  (assoc [this k v]
    (ImmutableCtrie. (.assoc (.snapshot ctrie) k v) _meta))

  (without [this k]
    (ImmutableCtrie. (.without (.snapshot ctrie) k) _meta))

  (assocEx [this k v]
    (if (contains? ctrie k)
      (throw (ex-info "Key already present"
               {:k k :v v :existing-v (get ctrie k)}))
      (ImmutableCtrie. (.assoc (.snapshot ctrie) k v) _meta)))

  (valAt [this k]
    (.valAt ctrie k))

  (valAt [this k not-found]
    (.valAt ctrie k not-found))

  (iterator [this]
    (.iterator ctrie))

  (seq [this]
    (.seq ctrie))

  (equiv [this that]
    (cond
      (identical? this that)
      true

      (not (instance? java.util.Map that))
      false

      (and (instance? IPersistentMap that)
           (not (instance? clojure.lang.MapEquivalence that)))
      false

      :else
      (let [m ^java.util.Map that]
        (if (== (.size m) (.count this))
          (loop [es (.seq this)]
            (if es
              (let [e ^java.util.Map$Entry (first es)
                    k (.getKey e)
                    found? (.containsKey m k)]
                (if (and found? (Util/equiv (.getValue e) (.get m k)))
                  (recur (next es))
                  false))
              true))
          false))))

  clojure.lang.IHashEq
  (hasheq [this]
    (hash-unordered-coll this))

  clojure.lang.IEditableCollection
  (asTransient [this]
    (.snapshot ctrie))

  clojure.lang.IDeref
  (deref [this]
    (.snapshot ctrie))

  clojure.lang.IObj
  (meta [this]
    _meta)

  (withMeta [this m]
    (ImmutableCtrie. ctrie m))

  clojure.lang.IFn
  (invoke [this k]
    (.valAt this k))

  (invoke [this k not-found]
    (.valAt this k not-found))

  (applyTo [this args]
    (let [n (RT/boundedLength args 2)]
      (case n
        0 (throw (clojure.lang.ArityException.
                  n (.. this (getClass) (getSimpleName))))
        1 (.invoke this (first args))
        2 (.invoke this (first args) (second args))
        3 (throw (clojure.lang.ArityException.
                  n (.. this (getClass) (getSimpleName)))))))

  java.util.Map
  (size [this]
    (.size ctrie))

  (get [this k]
    (.get ctrie k))

  (entrySet [this]
    (.entrySet ctrie))

  (containsKey [this k]
    (.containsKey ctrie k))

  (containsValue [this v]
    (.containsValue ctrie v))

  (values [this]
    (.values ctrie))

  (put [this k v]
    (throw (UnsupportedOperationException.)))

  (putAll [this m]
    (throw (UnsupportedOperationException.)))

  (remove [this k]
    (throw (UnsupportedOperationException.)))

  (clear [this]
    (throw (UnsupportedOperationException.)))

  Object
  (hashCode [this]
    (APersistentMap/mapHash this))

  (equals [this that]
    (.equals ctrie that)))

(defmethod print-method ImmutableCtrie
  [^ImmutableCtrie ictrie ^java.io.Writer w]
  (map-print-method ictrie w))

(doseq [v [#'->Ctrie
           #'->CtrieIterator
           #'->ImmutableCtrie
           #'->Descriptor
           #'->Failed
           #'->Gen
           #'->Just
           #'->Nothing
           #'->IndirectionNode
           #'->BitmapIndexedNode
           #'->HashCollisionNode
           #'->SingletonNode
           #'->TombNode
           #'->RESTART
           #'->NOTFOUND
           #'->KEYABSENT
           #'->KEYPRESENT]]
  (alter-meta! v assoc :private true))

(defn ^Ctrie concurrent-map
  "Returns a new concurrent map containing the given keyvals."
  ([]
     (Ctrie. (new-root-node) false))
  ([& keyvals]
     (let [out ^Ctrie (concurrent-map)]
       (loop [in (seq keyvals)]
         (if in
           (if-let [nin (next in)]
             (let [k (first in)
                   h (hash k)]
               (.insertHash out (first in) (first nin) h)
               (recur (next nin)))
             (throw (IllegalArgumentException.
                     (str "No value supplied for key: " (pr-str (first in))))))
           out)))))
