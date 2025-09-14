-- Black MID 고객 정보 조회 쿼리
-- 파라미터: :mid_list (MID 리스트를 IN 절에 삽입)
SELECT DISTINCT
    -- CID
    c.CUST_ID AS "CID",
    
    -- 이름(성명)
    c.CUST_KO_NM AS "이름",
    
    -- 성별
    (SELECT AML_DTL_CD_NM 
     FROM SM_CD_DTL 
     WHERE AML_COMN_CD = 'CUST_GNDR_CD' 
       AND AML_DTL_CD = c.CUST_GNDR_CD) AS "성별",
    
    -- 생년월일
    c.CUST_BDAY AS "생년월일",
    
    -- 고액자산가
    c.CLB_YN AS "고액자산가",
    
    -- 거주지 정보 (주소 + 상세주소)
    CASE 
        WHEN c.CUST_DTL_ADDR IS NOT NULL 
        THEN c.CUST_ADDR || ' ' || c.CUST_DTL_ADDR
        ELSE c.CUST_ADDR
    END AS "거주지정보",
    
    -- 직장명
    c.WPLC_NM AS "직장명",
    
    -- 직장 정보 (직장 주소 + 상세주소)
    CASE 
        WHEN c.WPLC_DTL_ADDR IS NOT NULL 
        THEN c.WPLC_ADDR || ' ' || c.WPLC_DTL_ADDR
        ELSE c.WPLC_ADDR
    END AS "직장정보",
    
    -- 핸드폰 번호 (연락처) - AES 복호화
    AES_DECRYPT(c.CUST_TEL_NO) AS "핸드폰번호",
    
    -- E-mail 주소 - AES 복호화
    AES_DECRYPT(c.CUST_EMAIL) AS "이메일주소",
    
    -- KYC 완료일시
    TO_CHAR(c.KYC_EXE_FNS_DTM, 'YYYY-MM-DD HH24:MI:SS') AS "KYC완료일시",
    
    -- MID 정보 추가
    COALESCE(m.MEM_ID, c.KYC_EXE_MEM_ID) AS "MID"
    
FROM BTCAMLDB_OWN.KYC_CUST_BASE c

-- MID 정보를 가진 회원 테이블과 조인
LEFT JOIN BTCAMLDB_OWN.KYC_MEM_BASE m
    ON c.CUST_ID = m.CUST_ID

WHERE 
    -- 개인 고객만 조회 (CUST_TYPE_CD = '01')
    c.CUST_TYPE_CD = '01'
    
    -- MID가 블랙리스트에 포함된 경우
    AND (
        m.MEM_ID IN (:mid_list)
        OR c.KYC_EXE_MEM_ID IN (:mid_list)
    )

ORDER BY c.CUST_ID