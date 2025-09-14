-- 사용자 가입일자 조회
-- 파라미터: 
--   :user_ids - 사용자 ID 리스트 (IN 절에 사용)
SELECT 
    user_id, 
    user_rgst_date AS join_datetime, 
    user_addr
FROM fms.BDM_MEM_ID_INFO
WHERE user_id IN (:user_ids)