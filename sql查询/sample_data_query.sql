SET hive.mapred.mode=nonstrict;
SET hive.strict.checks.large.query=false;

WITH EventMapping AS (
    -- 通过URL映射事件类型、事件名称和页面名称
    SELECT '/expose/tutorNewPc/homepageExpose' AS url, '曝光' AS event_type, '我的课程/图书页面曝光' AS event_name, '我的课程页' AS page_name
    UNION ALL SELECT '/click/tutorNewPc/seasonCardClick', '点击', '班课卡片点击', '我的课程页'
    UNION ALL SELECT '/click/tutorNewPc/searchClick', '点击', '搜索点击', '我的课程页'
    UNION ALL SELECT '/click/tutorNewPc/mentorClick', '点击', '班主任辅导点击', '我的课程页'
    UNION ALL SELECT '/click/tutorNewPc/homepageClick', '点击', '我的课程/图书切tab点击', '我的课程页'
    UNION ALL SELECT '/click/tutorNewPc/hideEpisodeClick', '点击', '已隐藏课程点击', '我的课程页'
    UNION ALL SELECT '/click/tutorNewPc/filterClick', '点击', '筛选项点击', '我的课程页'
    UNION ALL SELECT '/click/userInfo/userInfoClick', '点击', '个人中心点击', '我的课程页'
    UNION ALL SELECT '/click/userInfo/function', '点击', '个人中心功能点击', '我的课程页'
    UNION ALL SELECT '/expose/seasonPage/seasonPageExpose', '曝光', '班课主页曝光', '班课主页'
    UNION ALL SELECT '/click/seasonPage/coursewareClick', '点击', '课件点击', '班课主页'
    UNION ALL SELECT '/click/seasonPage/episodeClick', '点击', '直播/录播课程点击', '班课主页'
    UNION ALL SELECT '/click/seasonPage/episodeLinkClick', '点击', '课程环节点击', '班课主页'
    UNION ALL SELECT '/click/seasonPage/episodeSyllabusClick', '点击', '课程大纲点击', '班课主页'
    UNION ALL SELECT '/click/seasonPage/functionType', '点击', '金刚位功能点击', '班课主页'
    UNION ALL SELECT '/click/seasonPage/moreClick', '点击', '「更多」点击', '班课主页'
    UNION ALL SELECT '/click/seasonPage/reportClick', '点击', '报告点击', '班课主页'
    UNION ALL SELECT '/click/seasonPage/toolClick', '点击', '工具栏功能点击', '班课主页'
)
-- 直接从源表中为所有指定的URL获取前100条样本数据
SELECT
    *
FROM
    dw_dwd.dwd_tutor_frog_di
WHERE
    dt = '2025-07-03' -- 将查询范围缩小到一天，以减少扫描的数据量
    AND url IN (SELECT url FROM EventMapping)
LIMIT 100;