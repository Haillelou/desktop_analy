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
),
RawData AS (
    -- 从主事件表中筛选出指定日期范围和URL的原始数据
    SELECT
        dt,
        user_id,
        url
    FROM
        dw_dwd.dwd_tutor_frog_di
    WHERE
        dt BETWEEN '2025-05-20' AND '2025-07-03'
        AND url IN (SELECT url FROM EventMapping)
),
DailyPageExposures AS (
    -- 计算每个页面每日的独立访客曝光数（UV）
    SELECT
        r.dt,
        em.page_name,
        COUNT(DISTINCT r.user_id) AS total_exposures
    FROM
        RawData r
    JOIN
        EventMapping em ON r.url = em.url
    WHERE
        em.event_type = '曝光'
    GROUP BY
        r.dt,
        em.page_name
),
DailyFeatureClicks AS (
    -- 计算每个功能每日的独立访客点击数（UV）
    SELECT
        r.dt,
        em.page_name,
        em.event_name,
        COUNT(DISTINCT r.user_id) AS total_clicks
    FROM
        RawData r
    JOIN
        EventMapping em ON r.url = em.url
    WHERE
        em.event_type = '点击'
    GROUP BY
        r.dt,
        em.page_name,
        em.event_name
)
-- 将曝光和点击数据关联，计算点击率（CTR）
SELECT
    fc.dt AS `日期`,
    fc.page_name AS `页面名称`,
    fc.event_name AS `功能名称`,
    fc.total_clicks AS `点击UV`,
    pe.total_exposures AS `曝光UV`,
    CASE
        WHEN pe.total_exposures > 0 THEN ROUND(fc.total_clicks * 100.0 / pe.total_exposures, 4)
        ELSE 0
    END AS `点击率(%`
FROM
    DailyFeatureClicks fc
LEFT JOIN
    DailyPageExposures pe ON fc.dt = pe.dt AND fc.page_name = pe.page_name
ORDER BY
    `日期`, `页面名称`, `点击UV` DESC;