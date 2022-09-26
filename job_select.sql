
SELECT 
  tjm.tg_job_mst_id  -- [일작업ID] 일일 대상 작업 MST ID   
  ,tjm.job_exec_dt   -- [작업실행일자] DB 일자 or SYSTEM   
  ,tjd.tg_job_dtl_id -- [일작업상세ID]    
  ,tjd.job_id        -- [작업ID] JOB ID Unique    
  ,tjd.run_type      -- [실행유형] Run, Stop, ReRun, Skip, Force Run    
  ,mj.job_cycl       -- [실행주기] D:Daily,W:Weekly,M:Monthly,L:LastDay
  ,CASE WHEN mj.exec_dt IN (null, '') THEN '00' 
   ELSE mj.exec_dt 
   END AS exec_dt    -- [실행일자] monthly: 실행일자, weekly : 요일구분, Daily  일경우 '00'
  ,mj.exec_time      -- [실행시각] 0000~2359       
  ,mj.timeout        -- [제한시간] 작업 Timeout 시각(알람 설정 시 사용)    
  ,mj.alarm_yn       -- [알림여부]    
  ,CASE WHEN alarm_yn ='Y' THEN mj.alarm_type        
   ELSE 'N/A'  
   END AS alarm_type  -- [알림유형] msg, email, talk 등
  ,ms.sh_id        -- [쉘 ID] Shell ID Unique              
  ,ms.sh_nm        -- [쉘명] 쉘스크립트 이름                       
  ,ms.work_div_1   -- [작업구분1] 계정백업 등                      
  ,ms.work_div_2   -- [작업구분2] 정보백업 등                      
  ,ms.svr_id       -- [서버 ID] 서버 ID Unique                
  ,ms.svr_nm       -- [서버명] 서버명                           
  ,ms.ip_addr      -- [IP 정보] ipv4                        
  ,ms.db_id        -- [DB ID]                             
  ,ms.db_nm        -- [DB 명]                              
  ,ms.back_type    -- [백업유형] incr / arch / full / merge   
  ,ms.sh_file_nm   -- [쉘 파일명] Real 파일명                    
  ,ms.sh_path      -- [쉘 경로] 쉘스크립트 경로                     
  ,msi.svr_hostname -- hostname  
  ,moi.svr_hostname AS db_hostname  -- hostname  
  ,COALESCE(msi.svr_alias,'') AS svr_alias -- 서버 별칭
  ,msi.svr_ip_v4            -- 서버 IP
  ,moi.svr_ip_v4 AS db_ip   -- DB서버 IP
  ,moi.ora_sid              -- SID     
  ,moi.ora_home             -- ORACLE HOME
  ,COALESCE(moi.ora_alias,'') AS ora_alias -- DB 별칭
  ,moi.ora_biz_dep_a        -- 업무그룹 1
  ,moi.ora_biz_dep_b        -- 업무그룹 2
  ,moi.db_name              -- DB명 
FROM 
  store.tg_job_mst tjm 
  INNER JOIN 
  store.tg_job_dtl tjd 
  ON ( 
      tjm.tg_job_mst_id = tjd.tg_job_mst_id 
      AND tjd.use_yn='Y' 
      AND tjd.run_type ='Run'
    )
  INNER JOIN 
  master.mst_job mj 
  ON ( 
    mj.use_yn ='Y'
    AND mj.job_id = tjd.job_id        
  )
  INNER JOIN 
  master.mst_shell ms 
  ON (
    ms.use_yn ='Y'
    AND ms.sh_id = mj.sh_id      
  )
  INNER JOIN 
  master.master_svr_info msi 
  ON (msi.svr_id = ms.svr_id )
  INNER JOIN 
  master.master_ora_info moi 
  ON (
    moi.svr_id = msi.svr_id 
    AND moi.ora_id = ms.db_id 
  )  
WHERE 
  tjm.use_yn ='Y'
 -- AND tjm.job_exec_dt = to_char(now(), 'YYYYMMDD') -- 실행일자로 변경하여 사용   
  AND tjm.job_exec_dt = '20201023' -- 실행일자로 변경하여 사용   

and tjd.job_id  = 13




 
    
  
   