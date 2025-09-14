-- 일별/종목별/계정별 거래 상세 쿼리
-- 파라미터:
--   :user_ids - 사용자 ID 리스트 (IN 절에 사용)
--   :start_time - 조회 시작 시간 (YYYY-MM-DD HH24:MI:SS)
--   :end_time - 조회 종료 시간 (YYYY-MM-DD HH24:MI:SS)

SELECT 
    user_id,
    TO_CHAR(trade_date, 'YYYY-MM-DD') AS trade_date,
    market_nm,
    ticker_nm,
    SUM(CASE WHEN trans_cat = 1 THEN trade_quantity ELSE 0 END) AS buy_quantity,
    SUM(CASE WHEN trans_cat = 2 THEN trade_quantity ELSE 0 END) AS sell_quantity,
    SUM(CASE WHEN trans_cat = 1 THEN trade_amount_krw ELSE 0 END) AS buy_amount_krw,
    SUM(CASE WHEN trans_cat = 2 THEN trade_amount_krw ELSE 0 END) AS sell_amount_krw,
    SUM(trade_amount_krw) AS total_amount_krw,
    AVG(CASE WHEN trans_cat = 1 THEN trade_price END) AS avg_buy_price,
    AVG(CASE WHEN trans_cat = 2 THEN trade_price END) AS avg_sell_price,
    COUNT(CASE WHEN trans_cat = 1 THEN 1 END) AS buy_count,
    COUNT(CASE WHEN trans_cat = 2 THEN 1 END) AS sell_count,
    COUNT(*) AS total_trades
FROM fms.BDM_VRTL_AST_TRAN_LEDG_FACT
WHERE user_id IN (:user_ids)
    AND trade_date >= :start_time
    AND trade_date < :end_time
GROUP BY 
    user_id,
    TO_CHAR(trade_date, 'YYYY-MM-DD'),
    market_nm,
    ticker_nm
ORDER BY 
    user_id,
    trade_date,
    market_nm,
    ticker_nm