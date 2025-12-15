BEGIN
    -- A. Seed 50 Products
    FOR i IN 1..50 LOOP
        INSERT INTO products (product_name, category, unit_price)
        VALUES (
            'Product_' || i, 
            CASE MOD(i, 3) WHEN 0 THEN 'Electronics' WHEN 1 THEN 'Home' ELSE 'Clothing' END,
            ROUND(DBMS_RANDOM.VALUE(10, 500), 2)
        );
    END LOOP;

    -- B. Seed 500 Customers
    FOR i IN 1..500 LOOP
        INSERT INTO customers (cust_name, signup_date)
        VALUES (
            'Customer_' || i, 
            TRUNC(SYSDATE - DBMS_RANDOM.VALUE(1, 1000))
        );
    END LOOP;

    -- C. Generate 10,000 Transactions (The Complex Dataset)
    FOR i IN 1..10000 LOOP
        INSERT INTO sales_transactions (customer_id, product_id, quantity, txn_date)
        VALUES (
            ROUND(DBMS_RANDOM.VALUE(1, 500)), -- Random Customer ID
            ROUND(DBMS_RANDOM.VALUE(1, 50)),  -- Random Product ID
            ROUND(DBMS_RANDOM.VALUE(1, 5)),   -- Random Quantity (1-5)
            TRUNC(SYSDATE - DBMS_RANDOM.VALUE(0, 365)) -- Random date in last year
        );
    END LOOP;

    COMMIT;
    DBMS_OUTPUT.PUT_LINE('Data generation complete: 10,000 rows inserted.');
END;
/