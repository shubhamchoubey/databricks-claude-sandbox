SET SERVEROUTPUT ON;

BEGIN
    -- 1. Check counts before processing (All should be Bronze default)
    DBMS_OUTPUT.PUT_LINE('Gold Customers before: ' || cust_analytics_pkg.get_tier_count('GOLD'));
    
    -- 2. Run the complex procedure
    cust_analytics_pkg.process_customer_tiers;
    
    -- 3. Check counts after processing
    DBMS_OUTPUT.PUT_LINE('Gold Customers after: ' || cust_analytics_pkg.get_tier_count('GOLD'));
    DBMS_OUTPUT.PUT_LINE('Silver Customers after: ' || cust_analytics_pkg.get_tier_count('SILVER'));
END;
/