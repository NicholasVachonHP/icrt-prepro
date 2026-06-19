"""Gold-layer quality evaluation for the contract intelligence pipeline.

Compares the structured fields produced by ``gold/fields.py`` against a
hand-authored ground-truth dataset (``config/eval/contract_truth.json``) and
reports per-field / per-contract accuracy. See :mod:`.gold_eval`.
"""
