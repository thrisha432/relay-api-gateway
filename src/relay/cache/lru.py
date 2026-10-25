"""A from-scratch LRU cache: hash map + doubly linked list.

Layout::

    head <-> [MRU] <-> ... <-> [LRU] <-> tail      (head/tail are sentinels)

* ``_map`` maps key -> node, giving average O(1) lookup.
* The list keeps recency order. Moving a node to the front or unlinking the
  LRU node touches a constant number of pointers, so ``get``, ``put``, and
  eviction are all average O(1) (the "average" comes from the dict).
* Sentinels mean there are no ``None`` checks for empty/edge positions.

Invariants (verified by ``check_invariants`` in tests):
1. ``len(_map)`` equals the number of real nodes in the list.
2. Every node in the list is in ``_map`` under its own key, and vice versa.
3. For every node, ``node.next.prev is node`` and ``node.prev.next is node``.
4. ``len(self) <= capacity``.

Semantics:
* ``get`` on a hit marks the key most-recently-used; a miss returns ``default``.
* ``put`` on an existing key replaces the value and marks it MRU (no eviction).
* ``put`` of a new key at capacity evicts exactly the LRU entry first.
* capacity 0 is a valid "disabled" cache: ``put`` stores nothing.

Not thread-safe; Relay uses it from a single asyncio event loop, and no method
awaits, so each call is atomic with respect to other coroutines.
"""

from __future__ import annotations

from collections.abc import Callable, Iterator


class _Node[K, V]:
    __slots__ = ("key", "next", "prev", "value")

    def __init__(self, key: K, value: V) -> None:
        self.key = key
        self.value = value
        self.prev: _Node[K, V] = self
        self.next: _Node[K, V] = self


class LRUCache[K, V]:
    def __init__(self, capacity: int, on_evict: Callable[[K, V], None] | None = None) -> None:
        if capacity < 0:
            raise ValueError("capacity must be >= 0")
        self._capacity = capacity
        self._on_evict = on_evict
        self._map: dict[K, _Node[K, V]] = {}
        # Sentinels carry no real key/value; they are never stored in _map.
        self._head: _Node[K, V] = _Node(None, None)  # type: ignore[arg-type]
        self._tail: _Node[K, V] = _Node(None, None)  # type: ignore[arg-type]
        self._head.next = self._tail
        self._tail.prev = self._head

    # ------------------------------------------------------------ public API

    @property
    def capacity(self) -> int:
        return self._capacity

    def __len__(self) -> int:
        return len(self._map)

    def __contains__(self, key: object) -> bool:
        """Membership test that does NOT change recency."""
        return key in self._map

    def get(self, key: K, default: V | None = None) -> V | None:
        node = self._map.get(key)
        if node is None:
            return default
        self._move_to_front(node)
        return node.value

    def peek(self, key: K, default: V | None = None) -> V | None:
        """Read without changing recency."""
        node = self._map.get(key)
        return default if node is None else node.value

    def put(self, key: K, value: V) -> tuple[K, V] | None:
        """Insert or update. Returns the evicted (key, value), if any."""
        if self._capacity == 0:
            return None
        node = self._map.get(key)
        if node is not None:
            node.value = value
            self._move_to_front(node)
            return None
        evicted = None
        if len(self._map) >= self._capacity:
            evicted = self._evict_lru()
        node = _Node(key, value)
        self._map[key] = node
        self._link_front(node)
        return evicted

    def delete(self, key: K) -> bool:
        node = self._map.pop(key, None)
        if node is None:
            return False
        self._unlink(node)
        return True

    def clear(self) -> None:
        self._map.clear()
        self._head.next = self._tail
        self._tail.prev = self._head

    def keys(self) -> Iterator[K]:
        """Keys from most- to least-recently used."""
        node = self._head.next
        while node is not self._tail:
            yield node.key
            node = node.next

    # ------------------------------------------------------- list operations

    def _link_front(self, node: _Node[K, V]) -> None:
        first = self._head.next
        node.prev = self._head
        node.next = first
        first.prev = node
        self._head.next = node

    @staticmethod
    def _unlink(node: _Node[K, V]) -> None:
        node.prev.next = node.next
        node.next.prev = node.prev
        node.prev = node.next = node

    def _move_to_front(self, node: _Node[K, V]) -> None:
        if self._head.next is node:
            return
        self._unlink(node)
        self._link_front(node)

    def _evict_lru(self) -> tuple[K, V]:
        lru = self._tail.prev
        assert lru is not self._head, "evict called on empty cache"
        self._unlink(lru)
        del self._map[lru.key]
        if self._on_evict is not None:
            self._on_evict(lru.key, lru.value)
        return lru.key, lru.value

    # ------------------------------------------------------------- testing

    def check_invariants(self) -> None:
        """Raise AssertionError if the map and list disagree in any way."""
        seen = 0
        node = self._head
        while node.next is not self._tail:
            nxt = node.next
            assert nxt.prev is node, "broken back-pointer"
            assert self._map.get(nxt.key) is nxt, "list node missing from map"
            seen += 1
            assert seen <= len(self._map), "cycle or extra nodes in list"
            node = nxt
        assert self._tail.prev is node, "tail back-pointer broken"
        assert seen == len(self._map), "map has nodes not in list"
        assert len(self._map) <= self._capacity or self._capacity == 0
