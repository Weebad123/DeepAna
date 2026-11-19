SELECT t.status, COUNT(*) AS txs, SUM(t.fee) AS fees
FROM solana.transactions AS t
WHERE t.slot >= {{from_slot}} AND t.slot <= {{to_slot}}
GROUP BY t.status
