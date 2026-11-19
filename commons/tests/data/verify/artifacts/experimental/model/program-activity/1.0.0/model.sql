-- Instructions per program in the window, busiest first.
SELECT i.program_id, COUNT(*) AS n
FROM solana.instructions AS i
WHERE i.slot BETWEEN {{from_slot}} AND {{to_slot}}
GROUP BY i.program_id
ORDER BY n DESC, i.program_id
