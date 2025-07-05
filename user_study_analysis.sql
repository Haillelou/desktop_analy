-- 用户学习分析查询
-- 创建时间: 2024年

WITH 
-- 班课信息表
season_t AS (
    SELECT 
        seasonid,
        concat_ws(',',collect_set(teacher_nickname)) as teacher_nickname,
        concat_ws(',',collect_set(teacher_ldap)) as teacher_ldap,
        concat_ws(',', sort_array(collect_set(season_week))) as season_week,
        concat_ws(',', sort_array(collect_set(season_time))) as season_time
    FROM tutor.dw_season_order_information_new
    WHERE semesterid = '141'
        AND courseid in ('1','2','3','204','4','207')
        AND grade_type in ('xiaoxue','chuzhong','gaozhong')
    GROUP BY seasonid
),
-- 作业/笔记统计表
submit_t AS (
    SELECT user_id as userid,
           season_id as seasonid,episode_id,
           count(distinct if(is_finish_homework=1,episode_id,null)) as homework_cnt,
           count(distinct if(is_finish_note=1,episode_id,null)) as note_cnt
      FROM dw_dws.dws_tutor_user_after_episode_activity_info_da
     WHERE dt = date_sub(current_date,1)
       AND to_date(from_unixtime(int(episode_start_time/1000))) between '2025-06-13' and '2025-07-04'
     GROUP BY user_id, season_id,episode_id
),
-- 分层随堂探究统计表
fenceng_t AS (
  SELECT a.userid,
       a.seasonid,episodeid,
       count(distinct episodeid) as kehou_should_answer_cnt, 
       count(case when is_finish='1' then 1 else null end) as kehou_answer_cnt,
       sum(case when kehou_answer_right_cnt is not null then kehou_answer_right_cnt else 0 end) as kehou_answer_right_cnt
  FROM 
  (
    SELECT DISTINCT 
       userid,
       seasonid,
       episodeid
    FROM tutor.dw_tutor_season_order_snapshot_da_view 
    WHERE dt = '2025-07-04'
      AND order_paidtime > '0'       
      AND refundedtime = '0'       
      AND is_normal_order = '1'                
      AND is_inner_user = '0'         
      AND is_test = '0'          
      AND semesterid = '141'
      AND courseid in ('1','2','3','204','4','207')
      AND grade_type in ('xiaoxue','chuzhong','gaozhong') 
      AND seasonname NOT RLIKE '测试'           
      AND to_date(from_unixtime(int(order_paidtime/1000))) <= '2025-07-04'
      AND to_date(from_unixtime(int(episode_starttime/1000))) <=  '2025-07-04'
      AND to_date(from_unixtime(int(episode_starttime/1000))) >= to_date(from_unixtime(int(order_paidtime/1000)))
      AND source NOT IN ('12','22','19')         
      AND lessonmarkid NOT IN('63', '71')               
      AND lesson_episode_label_id = '1000010'
  ) a 
  LEFT JOIN (SELECT userid,
         sourceid,
         is_finish,
         count(distinct case when answerStatus='1' then questionid else null end) as kehou_answer_right_cnt
    FROM 
    (
      SELECT userid,
           sourceid,
           is_finish,
           get_json_object(get_json_object(question_list, '$.questionEntity'),'$.questionId') AS questionid,
           get_json_object(get_json_object(question_list, '$.marking'),'$.answerStatus') AS answerStatus
      FROM (
        SELECT
            a.userid,
            a.sourceid,
            a.exerciseid,
            a.is_finish,
            b.answersheetreportid,
            q_list.question_list_item AS question_list -- Alias back to question_list for outer query compatibility
        FROM (
            SELECT
                userid,
                sourceid,
                exerciseid,
                CASE WHEN status IN ('2','3') THEN '1' ELSE '0' END AS is_finish
            FROM dw_ods.ods_mysql_tutor_modular_exercise_user_sub_modular_exercise_da
            WHERE dt = date_sub(current_date, 1)
              AND exercisetype = '32'
        ) a
        LEFT JOIN tutor.ods_tutor_lesson_exercise_exercise_da b
            ON a.exerciseid = b.id
        LEFT JOIN tutor.ods_tutor_answer_sheet_report_tutor_answer_sheet_report_da c
            ON b.answersheetreportid = c.id
        LATERAL VIEW explode(split(
            regexp_replace(
                substr(c.compressedquestionmarkings, 2, length(c.compressedquestionmarkings) - 2),
                '\\},\\{',
                '}#{'
            ),
            '#'
        )) q_list AS question_list_item
        WHERE b.dt = date_sub(current_date, 1)
      ) base
    ) S1_data
    GROUP BY
           userid,
           sourceid,
           is_finish
  ) afterclass_t
  ON a.episodeid=afterclass_t.sourceid AND a.userid=afterclass_t.userid
  GROUP BY
          a.userid,
          a.seasonid,episodeid
),
ab as (
    select distinct userid,lesson_id,abtest
    from dw_dwd.desktop_try
)
attend_finish_t AS (
    SELECT 
        userid,
        seasonid,episodeid,episode_start_dt,
        count(distinct if(is_should_attend=1,episodeid,null)) as should_attend_cnt,
        count(distinct if(is_attend=1,episodeid,null)) as live_attend_cnt,
        count(distinct if(is_finish=1,episodeid,null)) as live_finish_cnt,
        count(distinct if(is_replay_attend=1,episodeid,null)) as replay_attend_cnt,
        count(distinct if(is_replay_finish=1,episodeid,null)) as replay_finish_cnt,
        count(distinct if(is_pseudo_should_attend=1,episodeid,null)) as pseudo_should_attend_cnt,
        count(distinct if(is_pseudo_attend=1,episodeid,null)) as pseudo_attend_cnt,
        count(distinct if(is_pseudo_finish=1,episodeid,null)) as pseudo_finish_cnt,
        count(distinct if(is_finish=1 OR is_replay_finish=1 OR is_pseudo_finish=1,episodeid,null)) as total_finish_cnt
    FROM
    (
        SELECT 
            finish_t.user_id as userid,
            finish_t.lesson_id as seasonid,
            finish_t.episode_id as episodeid,
            finish_t.episode_start_dt,
            1 as is_should_attend,
            if(finish_t.lesson_episode_label_id='1000010' and finish_t.is_attend_live='1', 1, 0) as is_attend,
            if(finish_t.lesson_episode_label_id='1000010' and finish_t.is_attend_live='1' and (finish_t.live_duration/finish_t.episode_duration>=0.3 or finish_t.live_duration/60000>=30), 1, 0) as is_finish,
            if(finish_t.lesson_episode_label_id='1000010' and finish_t.is_replay='1', 1, 0) as is_replay_attend,
            if(finish_t.lesson_episode_label_id='1000010' and finish_t.is_replay='1' and (finish_t.replay_duration/finish_t.episode_duration>=0.3 or finish_t.replay_duration/60000>=30), 1, 0) as is_replay_finish,
            if(finish_t.lesson_episode_label_id='1000010' and pseudofinish_t.episodeid is not null, 1, 0) as is_pseudo_should_attend,
            if(finish_t.lesson_episode_label_id='1000010' and pseudofinish_t.is_attend_live='1', 1, 0) as is_pseudo_attend,
            if(finish_t.lesson_episode_label_id='1000010' and pseudofinish_t.is_attend_live='1' and (pseudofinish_t.live_duration/pseudofinish_t.episode_duration>=0.3 or pseudofinish_t.live_duration/60000>=30), 1, 0) as is_pseudo_finish
        FROM tutor.dw_tutor_lesson_episode_user_finish_class_da finish_t
        LEFT JOIN 
        (
            SELECT DISTINCT a.lessonid
                , a.episodeid
                , a.pseudolivelessonid
                , a.pseudoliveepisodeid
                , b.semester_id as semesterid 
                , b.grade_type 
                , b.grade as gradeid
                , b.lesson_subject_id as courseid
                , b.user_id as userid
                , b.is_attend_live
                , b.live_duration
                , b.episode_duration
               -- ,b.episode_start_dt
                , hour(from_unixtime(int(b.episode_start_tp/1000))) as epi_hour 
                , CASE b.episode_week_day 
                    WHEN 1 THEN '周一'
                    WHEN 2 THEN '周二'
                    WHEN 3 THEN '周三'
                    WHEN 4 THEN '周四'
                    WHEN 5 THEN '周五'
                    WHEN 6 THEN '周六'
                    WHEN 7 THEN '周日'
                    ELSE b.episode_week_day 
                    END as episode_week_day
            FROM dw_ods.ods_tutor_pseudo_live_pseudo_live_lesson_da a 
            JOIN tutor.dw_tutor_lesson_episode_user_finish_class_da b ON a.pseudolivelessonid=b.lesson_id AND a.pseudoliveepisodeid=b.episode_id
            WHERE a.dt = date_sub(current_date,1)
                AND b.dt = date_sub(current_date,1)
                AND b.semester_id = '141'
                AND b.lesson_subject_id IN ('1','2','3','204','4','207')
                AND b.lesson_flat_category = '3'       
                AND b.lesson_mark_id NOT IN ('63', '71')        
                AND b.is_have_ticket = '1'             
                AND b.is_deleted = '0'         
        ) pseudofinish_t 
        ON finish_t.lesson_id = pseudofinish_t.lessonid 
            AND finish_t.episode_id = pseudofinish_t.episodeid
            AND finish_t.user_id = pseudofinish_t.userid 
        WHERE finish_t.dt = date_sub(current_date,1)
            AND finish_t.semester_id = '141'
            AND finish_t.lesson_subject_id in ('1','2','3','204','4','207')
            AND finish_t.lesson_flat_category = '3'
            AND finish_t.lesson_mark_id not in ('63', '71')
            AND finish_t.lesson_episode_label_id = '1000010'
            AND finish_t.is_have_ticket = '1'
            AND finish_t.is_deleted = '0'
            AND finish_t.episode_start_dt between '2025-06-13' and '2025-07-04'
    ) t
    GROUP BY  userid,seasonid,episodeid,episode_start_dt
),
mx as (
    SELECT 
    -- 1. 学生基本信息
    base_t.userid as userid,ab.abtest,
    CASE base_t.semesterid 
        WHEN 132 THEN '23寒'
        WHEN 133 THEN '23春'
        WHEN 134 THEN '23暑'
        WHEN 135 THEN '23秋'
        ELSE base_t.semesterid
    END as season, --学季
    CASE base_t.grade_type
        WHEN 'xiaoxue' THEN '小学'
        WHEN 'chuzhong' THEN '初中'
        WHEN 'gaozhong' THEN '高中'
        ELSE base_t.grade_type
    END as grade_type, --学部
    CASE base_t.gradeid 
        WHEN -3 THEN '新概念'
        WHEN -2 THEN '剑桥班' 
        WHEN 98 THEN '小班'
        WHEN 99 THEN '中班'
        WHEN 100 THEN '大班'
        WHEN 1 THEN '一年级' 
        WHEN 2 THEN '二年级' 
        WHEN 3 THEN '三年级' 
        WHEN 4 THEN '四年级' 
        WHEN 5 THEN '五年级' 
        WHEN 6 THEN '六年级' 
        WHEN 7 THEN '初一' 
        WHEN 8 THEN '初二' 
        WHEN 9 THEN '初三' 
        WHEN 10 THEN '高一' 
        WHEN 11 THEN '高二' 
        WHEN 12 THEN '高三' 
        ELSE base_t.gradeid 
    END as grade_name, --年级
    CASE base_t.courseid
        WHEN 1 THEN '语文' 
        WHEN 2 THEN '数学' 
        WHEN 3 THEN '英语' 
        WHEN 4 THEN '物理' 
        WHEN 5 THEN '化学' 
        WHEN 6 THEN '生物' 
        WHEN 7 THEN '历史' 
        WHEN 8 THEN '地理' 
        WHEN 9 THEN '政治' 
        WHEN 14 THEN '思想品德' 
        WHEN 15 THEN '科学'
        WHEN 101 THEN '文综' 
        WHEN 201 THEN '编程'
        WHEN 204 THEN '人文博雅'
        WHEN 206 THEN '阅读表达'
        WHEN 207 THEN '新思维'
        ELSE base_t.courseid 
    END as subject_name, ---学科

    -- 2. 课程信息
    base_t.seasonid as seasonid, --班级id
    base_t.seasonname as seasonname, --班课名
    season_t.season_week as season_week, --周几上课
    season_t.season_time as season_time, --上课时间
    season_t.teacher_nickname as teacher_nickname, --主讲姓名
    season_t.teacher_ldap as teacher_ldap, --主讲ldap

    episodeid,
    episode_start_dt,

    -- 3. 到课情况
    nvl(attend_finish_t.should_attend_cnt,0) as ydk, --`应到课次数`,
    nvl(attend_finish_t.live_attend_cnt,0) as live_dk, --`直播到课次数`,
    nvl(attend_finish_t.live_finish_cnt,0) as live_wk, --`直播完课次数`,
    nvl(attend_finish_t.replay_attend_cnt,0) as hf_dk, --`回放到课次数`,
    nvl(attend_finish_t.replay_finish_cnt,0) as hf_wk, --`回放完课次数`,
    nvl(attend_finish_t.total_finish_cnt,0) as total_wk, --`总完课次数`,

    -- 4. 作业/笔记统计
    nvl(submit_t.homework_cnt,0) as work_tj,-- `作业提交次数`,
    nvl(submit_t.note_cnt,0) as bj_tj,-- `笔记提交次数`,
    
    -- 5. 伪直播到完课情况
    nvl(attend_finish_t.pseudo_should_attend_cnt,0) as pse_ydk, -- `伪直播应到课次数`,
    nvl(attend_finish_t.pseudo_attend_cnt,0) as pse_dk, -- `伪直播到课次数`,
    nvl(attend_finish_t.pseudo_finish_cnt,0) as pse_wk -- `伪直播完课次数`,
    
    -- 6. 分层随堂探究提交情况
    --nvl(fenceng_t.kehou_should_answer_cnt,0) as ,-- `分层随堂探究应提交次数`,
    --nvl(fenceng_t.kehou_answer_cnt,0) as ,-- `分层随堂探究提交次数`,
    --nvl(fenceng_t.kehou_answer_right_cnt,0) as ,-- `分层随堂探究作答正确题目数`

FROM 
(
    SELECT DISTINCT 
        userid,
        semesterid,
        seasonid,
        seasonname,
        courseid,
        grade_type,
        gradeid
    FROM tutor.dm_tutor_season_team_renewal_detail_da
    WHERE dt = '2025-07-04'
        AND semesterid = '141' 
        AND courseid in ('1','2','3','204','4','207')
        AND grade_type in ('xiaoxue','chuzhong','gaozhong')
        AND seasonname NOT RLIKE '测试'
        AND seasonid in (13424273,13424415,13424418,13424423,13424412,13424430,13424476,13424414,13504195,13504181,13504060,13504061,13504183,13506170,13426353,13426354,13426126,13426127,13426631,13426634)
) base_t
LEFT JOIN season_t ON base_t.seasonid = season_t.seasonid
left join ab on base_t.userid = ab.userid AND base_t.seasonid = ab.seasonid
LEFT JOIN attend_finish_t ON base_t.userid = attend_finish_t.userid AND base_t.seasonid = attend_finish_t.seasonid
LEFT JOIN submit_t ON base_t.userid = submit_t.userid AND base_t.seasonid = submit_t.seasonid and attend_finish_t.episodeid = submit_t.episodeid
LEFT JOIN fenceng_t ON base_t.userid = fenceng_t.userid AND base_t.seasonid = fenceng_t.seasonid and attend_finish_t.episodeid = fenceng_t.episodeid
    where ab.userid is not null
)
select abtest,
    season,grade_type,grade_name,subject_name,
    base_t.seasonid as 班级id,
    base_t.seasonname as 班课名,
    season_t.season_week as 周几上课,
    season_t.season_time as 上课时间,
    season_t.teacher_nickname as 主讲姓名,
    season_t.teacher_ldap as 主讲ldap,

    episodeid as 章节id,
    episode_start_dt as 章节上课时间,
    sum(live_dk) as 直播到课,
    sum(live_wk) as 直播完课,
    sum(ydk) as 应到课,
    sum(hf_dk) as 回放到课,
    sum(hf_wk) as 回放完课,
    sum(total_wk) as 总完课,
    sum(work_tj) as 作业提交,
    sum(bj_tj) as 笔记提交,
    sum(pse_ydk) as 伪直播应到课,
    sum(pse_dk) as 伪直播到课,
    sum(pse_wk) as 伪直播完课
from mx 
group by abtest,
    season,grade_type,grade_name,subject_name,
    base_t.seasonid ,
    base_t.seasonname ,
    season_t.season_week ,
    season_t.season_time ,
    season_t.teacher_nickname,
    season_t.teacher_ldap ,

    episodeid,
    episode_start_dt
