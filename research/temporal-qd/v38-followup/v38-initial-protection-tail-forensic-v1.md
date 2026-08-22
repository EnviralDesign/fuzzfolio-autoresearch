# V38 initial-protection tail forensic v1

Worst accepted child: `qd_e4dcc5fcc52872a11b24d532c1c5` parent `qd_ed27f99ba0a8dfd7c76c69687efb`
- mutationClass: `jump` site `stop`
- before: `{'kind': 'fixed_percent', 'percent': 1.0}`
- after: `{'kind': 'fixed_percent', 'percent': 0.25}`
- panel-3 cumulative net R: -69.00720422167237
- hypothesized mechanism: `cost_drag_and_churn`
- cost drag R: `51.99999999999999`
- close reason counts: `{'break_even_stop': 151, 'stop_loss': 77, 'take_profit': 28, 'take_profit_gap': 2, 'break_even_gap': 2}`
- close reason fractions: `{'break_even_gap': 0.007692307692307693, 'break_even_stop': 0.5807692307692308, 'stop_loss': 0.29615384615384616, 'take_profit': 0.1076923076923077, 'take_profit_gap': 0.007692307692307693}`
- trades: 260 worst losing streak 27

Full before→after decomposition is in the JSON report. Implied R:R is defined only for scalar stop/target locators.

Report sha: `sha256:3257855f719bc4ba330f957e98d280c86567de53e06c762b4f984c8e15155cc0`
