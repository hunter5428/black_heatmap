-- 사용자 접속 정보 조회
-- 파라미터: 
--   :user_ids - 사용자 ID 리스트 (IN 절에 사용)
--   :checkpoint_datetime - 조회 시작 일자 (YYYY-MM-DD)
SELECT
    user_id,
    LISTAGG(DISTINCT ip_addr, ',') AS ip_addr,
    LISTAGG(DISTINCT device_id, ',') AS device_id,
    LISTAGG(DISTINCT conn_os, ',') AS conn_os,
    LISTAGG(DISTINCT conn_brows, ',') AS conn_brows,
    LISTAGG(DISTINCT http_user_agent, ',') AS htts_uesr_agent
FROM fms.BDM_DLNG_LEDG_LIST_FACT
WHERE user_id IN (:user_ids)
    AND order_cat = 1
    AND order_date >= :checkpoint_datetime
GROUP BY user_id