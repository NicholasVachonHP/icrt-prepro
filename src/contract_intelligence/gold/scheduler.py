"""Global token-budget work runner for gold extraction.

Replaces the per-contract thread pool: every independent model call from every
contract is thrown into one pool, and a single in-flight *token* budget -- not a
fixed thread count -- decides how many run at once. The deployment caps tokens
per minute (TPM); admitting a call only when ``in_flight + cost <= budget`` keeps
momentary load under that ceiling and self-balances (many small chunk calls run
together, few large full-text / judge calls do not), unlike a fixed thread count.

:func:`run_token_budget` runs a set of *independent* tasks. The pipeline's two
dependency edges -- a list field's reduce after its map calls, and a contract's
judge after its extraction -- are honoured by running extraction, reduce, and
judge as three successive :func:`run_token_budget` stages; the stage boundary is
the dependency barrier, so the runner itself stays a simple independent-task pool.
"""

import threading
from concurrent.futures import ThreadPoolExecutor

# Absolute cap on OS threads. The token budget is the real throttle; this just
# bounds threads for the many-tiny-calls case (e.g. ~125 chunk maps fitting in a
# 500K budget) without spawning an unbounded pool.
_MAX_THREADS = 128


class _TokenGate:
    """Admit work while the summed in-flight token cost stays within budget.

    A task costing more than the whole budget is clamped to the budget so it runs
    alone rather than deadlocking; because the gate starts full, at least one task
    can always proceed, so the pool never stalls.
    """

    def __init__(self, budget):
        self._budget = max(1, int(budget))
        self._available = self._budget
        self._cond = threading.Condition()

    def acquire(self, cost):
        cost = min(max(1, int(cost)), self._budget)
        with self._cond:
            while self._available < cost:
                self._cond.wait()
            self._available -= cost
        return cost

    def release(self, cost):
        with self._cond:
            self._available += cost
            self._cond.notify_all()


def run_token_budget(tasks, budget, max_threads=_MAX_THREADS):
    """Run ``tasks`` under a global in-flight token budget; results in input order.

    Args:
        tasks: list of ``(est_tokens, callable)``. Each callable takes no args and
            returns its result. A callable should capture and return its own
            failures where the caller wants to continue (this runner lets
            exceptions propagate).
        budget: max summed ``est_tokens`` of concurrently running tasks.
        max_threads: hard cap on worker threads.
    """
    if not tasks:
        return []
    gate = _TokenGate(budget)
    results = [None] * len(tasks)

    def _run(idx):
        cost, fn = tasks[idx]
        held = gate.acquire(cost)
        try:
            results[idx] = fn()
        finally:
            gate.release(held)

    workers = min(max_threads, len(tasks))
    with ThreadPoolExecutor(max_workers=workers) as pool:
        list(pool.map(_run, range(len(tasks))))
    return results
