-- 4시간 단위 거래금액 집계 쿼리
-- 파라미터:
--   :user_ids - 사용자 ID 리스트 (IN 절에 사용)
--   :start_time - 조회 시작 시간 (YYYY-MM-DD HH24:MI:SS)
--   :end_time - 조회 종료 시간 (YYYY-MM-DD HH24:MI:SS)

SELECT 
    user_id,
    DATE_TRUNC('day', trade_date) + 
        INTERVAL '4 hours' * FLOOR(EXTRACT(hour FROM trade_date) / 4) AS time_slot,
    SUM(CASE WHEN trans_cat = 1 THEN trade_amount_krw ELSE 0 END) AS buy_amount_krw,
    SUM(CASE WHEN trans_cat = 2 THEN trade_amount_krw ELSE 0 END) AS sell_amount_krw,
    SUM(trade_amount_krw) AS total_amount_krw,
    COUNT(DISTINCT ticker_nm) AS traded_tickers,
    COUNT(*) AS trade_count
FROM fms.BDM_VRTL_AST_TRAN_LEDG_FACT
WHERE user_id IN (:user_ids)
    AND trade_date >= :start_time
    AND trade_date < :end_time
GROUP BY 
    user_id,
    DATE_TRUNC('day', trade_date) + 
        INTERVAL '4 hours' * FLOOR(EXTRACT(hour FROM trade_date) / 4)
ORDER BY 
    user_id, 
    time_slot