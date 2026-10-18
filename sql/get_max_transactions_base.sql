SELECT
    state,
    argMax(category, amount) AS category_of_largest_transaction,
    max(amount) AS largest_transaction_amount
FROM hw2.transactions
GROUP BY state
ORDER BY state;
