SELECT 
user_id
,url
,other['abtype'] as abgroup
,other['satisfactionlevel'] as satisfy
,client_tag_new
,device_type_new
,app_version
FROM 
    dw_dwd.dwd_tutor_frog_di                          
WHERE 
    dt BETWEEN '2025-05-20' AND '2025-07-02'                                   
    AND url IN ('/expose/satisfactionPopup/popupExpose','/click/satisfactionPopup/satisfactionClick')    -- 筛选特定 URL，确保只统计特定页面