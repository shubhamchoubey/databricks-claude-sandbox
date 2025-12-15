create or replace PACKAGE BODY SHUBHAM_CHOUBEY_SCHEMA_V4C5D.cust_analytics_pkg AS

    PROCEDURE process_customer_tiers IS
        -- 1. Complex Cursor with Aggregation and Joins
        CURSOR c_cust_spend IS
            SELECT c.customer_id, 
                   SUM(p.unit_price * t.quantity) AS total_spend
            FROM customers c
            JOIN sales_transactions t ON c.customer_id = t.customer_id
            JOIN products p ON t.product_id = p.product_id
            GROUP BY c.customer_id;

        -- Record variable to hold cursor row
        r_cust_spend c_cust_spend%ROWTYPE;

        v_new_tier VARCHAR2(20);
        v_update_count NUMBER := 0;

    BEGIN
        -- 2. Open and Iterate Cursor
        OPEN c_cust_spend;
        LOOP
            FETCH c_cust_spend INTO r_cust_spend;
            EXIT WHEN c_cust_spend%NOTFOUND;

            -- 3. Business Logic
            IF r_cust_spend.total_spend >= 3000 THEN
                v_new_tier := 'GOLD';
            ELSIF r_cust_spend.total_spend >= 1000 THEN
                v_new_tier := 'SILVER';
            ELSE
                v_new_tier := 'BRONZE';
            END IF;

            -- 4. Update Table
            UPDATE customers
            SET segment = v_new_tier
            WHERE customer_id = r_cust_spend.customer_id
            AND segment != v_new_tier; -- Only update if changed

            IF SQL%ROWCOUNT > 0 THEN
                v_update_count := v_update_count + 1;
            END IF;

        END LOOP;
        CLOSE c_cust_spend;

        COMMIT;
        DBMS_OUTPUT.PUT_LINE('Tier processing complete. Updated ' || v_update_count || ' customers.');

    EXCEPTION
        WHEN OTHERS THEN
            ROLLBACK;
            DBMS_OUTPUT.PUT_LINE('Error in process_customer_tiers: ' || SQLERRM);
    END process_customer_tiers;

    -- Helper function implementation
    FUNCTION get_tier_count(p_tier_name VARCHAR2) RETURN NUMBER IS
        v_count NUMBER;
    BEGIN
        SELECT COUNT(*) INTO v_count 
        FROM customers 
        WHERE segment = p_tier_name;

        RETURN v_count;
    END get_tier_count;

END cust_analytics_pkg;
