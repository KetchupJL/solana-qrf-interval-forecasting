 ## BigQuery Datastream Info:

 * **Holder Count (`token_balances`)**
  Counts how many unique wallets hold >0 of each token every 12 h. Use it to track sudden drops or surges in holders—sharp declines often foreshadow sell‐offs, while spikes can signal hype.

* **New Token Accounts (`token_accounts`)**
  Counts how many brand‐new token‐specific accounts appear (with nonzero balance) each 12 h. Treat it as a “hype indicator”: big jumps mean fresh wallets entering the market, which can precede a pump (or signal FOMO selling risk).

* **SPL-Token Instruction Count (`instructions`)**
  Totals all SPL-Token Program calls every 12 h (i.e. every on-chain SPL transfer across all tokens). Use it as a network-wide activity gauge—if your token’s transfers aren’t keeping pace with overall SPL traffic, its liquidity or tail risk may be mispriced.

* **Network Transaction Count (`transactions`)**
  Counts all Solana transactions every 12 h. Use it as a proxy for network congestion and fee pressure: high network-wide tx counts often mean higher fees and thinner alt-token volumes, which widens your forecasted return intervals.
