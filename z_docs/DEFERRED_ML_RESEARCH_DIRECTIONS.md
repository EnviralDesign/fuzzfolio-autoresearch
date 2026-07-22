# Deferred Machine-Learning Research Directions

## Status

Deferred until the first policy-honest Phase 3 PlayHand campaign reaches its
predeclared 10,000-lane boundary and the resulting corpus has been frozen and
audited. This document records research directions; it authorizes no ML model,
tail access, campaign-policy change, or deployment.

The broader long-term concept of a constrained temporal strategy language and
an inspectable learning architect is documented in
`STRATEGY_LANGUAGE_AND_LEARNING_ARCHITECT_VISION.md`.

## Why Revisit ML Now

Fuzzfolio now has infrastructure that earlier ML experiments lacked:

- a consistent five-year market-data lake;
- immutable experiment identities and data windows;
- distributed replay capacity;
- explicit execution-cost assumptions;
- durable records of successful, failed, and nonviable attempts;
- walk-forward, regime, and untouched-tail evaluation boundaries.

The effective sample size remains much smaller than the raw bar count. Five
years contains many observations but relatively few independent market
regimes. Flexible models can therefore overfit regime, instrument, spread,
calendar, or data-source artifacts more easily than the current strategy
grammar.

## Best Initial Applications

1. **Candidate prioritization** - predict which proposed PlayHand profiles are
   worth exact replay compute. The model orders work; replay remains the judge.
2. **Regime discovery** - cluster volatility, trend, correlation, and liquidity
   states, or model transitions with hidden Markov/change-point methods. Use
   the result to diagnose conditional dependence, not to excuse weak aggregate
   performance.
3. **Behavioral diversity** - embed strategy curves, holdings, and trade
   behavior to improve portfolio diversity beyond indicator-name differences.
4. **Surrogate optimization** - use Bayesian or other surrogate models to
   propose promising parameter regions, followed by exact replay validation.
5. **Data and deployment drift** - detect abnormal data and live behavior that
   no longer resembles the frozen research distribution.

## Direct Predictive Models

Start with regularized linear/logistic models and gradient-boosted trees.
Possible targets include forward return, probability of reward-before-stop,
or future excursion distributions. Treat predictions as indicators or gates
inside the existing strategy framework before considering fully model-driven
execution. Neural sequence models should come only after simple baselines.

Every direct model must use purged time-series validation, train/test embargo,
nested hyperparameter selection, frozen costs, instrument and regime reporting,
one untouched outer evaluation, and paper/live canary deployment.

## Search Algorithms

Evolutionary, novelty-search, pathfinding, and related algorithms may be useful
as proposal generators. Atlas can supply priors and PlayHand can remain the
common evaluation contract. New generators must not bypass the existing data,
cost, provenance, selection, or outer-test controls.

## Required First Dataset

Complete and freeze the first 10,000-lane Phase 3 campaign. Preserve guided,
uncertain, and wild policy assignments; proposals; intermediate sweeps;
calculated, rejected, no-signal, and nonviable outcomes; and selection
probabilities. Training only on promoted strategies would reproduce
survivorship bias.

## First Recommended Experiment

After the 10,000-lane forensic report, run a prospective candidate-ranking or
regime-discovery experiment. Freeze its training data and decision rule, then
compare it against the existing PlayHand policy on a subsequent campaign. Do
not use the reserved Phase 3 outer tail for model construction or iteration.
