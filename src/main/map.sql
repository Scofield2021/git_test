with t1 as (
    SELECT
        vin,
        is_user_vehicle,
        ota_version,
        year_model,
        CASE WHEN substr(year_model, 1, 4) < '2024' THEN 'SS2' ELSE 'SS3' END AS hard_ver
    from dm_vom_vehvoice_basic_df
    where dt = '{-dt}'
),

t2 as (select t.vin,
              t1.is_user_vehicle,
              t1.ota_version,
              t1.year_model,
              t1.hard_ver,
              create_time,
              receive_time,
              collect_time,
              veh_model,
              veh_config,
              veh_level,
              content,
              event_key,json_extract_scalar(json_extract_scalar(content, '$.data_json'), '$.record_id')
               AS record_id,
              cast(json_extract_scalar(content, '$.target_time') as BIGINT) as target_time,
              dt
       from ods_ssp_cloud_uploader_track_all_offline_di_prod t
                left join t1 on t.vin = t1.vin
       where dt = '-{dt}'
--and event_key = 'X_Map_001_0009'
         and json_extract_scalar(json_extract_scalar(content, '$.data_json'), '$.record_id') IS NOT NULL
)


select
    dt,
    date_parse(dt, '%Y-%m-%d') as dt_d,
    ota_version,
    hard_ver,
    t2.target_time as 001_0009_time,
    t_0008.target_time as 001_0008_time,
    t_0004.target_time as 007_0004_time,
    t_0005.target_time as ,
    t_0027.target_time,
    t_0028.target_time,
    t_0010.target_time,
    --avg(t2.target_time - t_0008.target_time) as costTime_avg,
    --approx_percentile(cast(t2.target_time - t_0008.target_time as double), 0.9) as costTime_p90,
    --approx_percentile(cast(t2.target_time - t_0008.target_time as double), 0.8) as costTime_p80
from t2
join (
    select
        record_id,
        target_time
    from t2
    where event_key = 'X_Map_001_0008'
) t_0008 on t2.record_id = t_0008.record_id
join  (
    select
        record_id,
        target_time
    from t2
    where event_key = 'X_Map_007_0004'
) t_0004 on t2.record_id = t_0004.record_id
join (
    select
        record_id,
        target_time
    from t2
    where event_key = 'X_Map_007_0005'
) t_0005 on t2.record_id = t_0005.record_id
join (
    select
        record_id,
        target_time
    from t2
    where event_key = 'X_Map_009_0027'
) t_0027 on t2.record_id = t_0005.record_id
join (
    select
        record_id,
        target_time
    from t2
    where event_key = 'X_Map_009_0028'
) t_0028 on t2.record_id = t_0008.record_id
join (
    select
        record_id,
        target_time
    from t2
    where event_key = 'X_Map_009_0010'
) t_0010 on t2.record_id = t_0010.record_id
where event_key = 'X_Map_001_0009'



------------------------
with t1 as (
    SELECT
    vin,
    is_user_vehicle,
    ota_version,
    year_model,
    CASE WHEN substr(year_model, 1, 4) < '2024' THEN 'SS2' ELSE 'SS3' END AS hard_ver
    from dm_vom_vehvoice_basic_df
    where dt = '{-dt}'
    ),

t2 as (
select
    t.vin,
    t1.is_user_vehicle,
    t1.ota_version,
    t1.year_model,
    t1.hard_ver,
    create_time,
    receive_time,
    collect_time,
    veh_model,
    veh_config,
    veh_level,
    content,
    event_key,
    get_json_object(get_json_object(content, '$.data_json'), '$.record_id') AS record_id,
    cast(get_json_object(content, '$.target_time') as BIGINT) as target_time,
    dt
    from ods_ssp_cloud_uploader_track_all_offline_di_prod t
    left join t1 on t.vin = t1.vin
    where dt = '-{dt}'
    and get_json_object(get_json_object(content, '$.data_json'), '$.record_id') IS NOT NULL
    )


select
    uuid() as uuid,
    now() as ts,
    dt,
    ota_version,
    hard_ver,
    sum(t2.target_time - t_0008.target_time) as costTime_sum,
    CAST(COUNT(DISTINCT record_id) AS bigint) AS record_cnt,
    approx_percentile(cast(t2.target_time - t_0008.target_time as double), 0.9) as costTime_p90,
    approx_percentile(cast(t2.target_time - t_0008.target_time as double), 0.8) as costTime_p80,
    approx_percentile(cast(t2.target_time - t_0008.target_time as double), 0.7) as costTime_p70,
    approx_percentile(cast(t2.target_time - t_0008.target_time as double), 0.6) as costTime_p60
from t2
join (
    select
        record_id,
        target_time
    from t2
    where event_key = 'X_Map_001_0008'
) t_0008 on t2.record_id = t_0008.record_id
where event_key = 'X_Map_001_0009'
group by dt, ota_version, hard_ver

-------------------------------------------------------------------------------------------------------------
select
    uuid() as uuid,
    now() as ts,
    dt,
    ota_version,
    hard_ver,
    X_Map_001_0009_time,
    X_Map_001_0008_time,
    X_Map_009_0027_time,
    X_Map_009_0028_time,
    X_Map_001_0010_time
from (
         select
             t_31.dt,
             ota_version,
             hard_ver,
             t_31.record_id,
             t_9.target_time as X_Map_001_0009_time,
             t_8.target_time as X_Map_001_0008_time,
             t_27.target_time as X_Map_009_0027_time,
             t_28.target_time as X_Map_009_0028_time,
             t_10.target_time as X_Map_001_0010_time
         from (
                  select
                      *,
                      get_json_object(get_json_object(content, '$.data_json'), '$.record_id') AS record_id
                  from ods_ssp_cloud_uploader_track_all_offline_di_prod
                  where dt = '{-dt}'
                    and event_key = 'vehvoice_002_0031'
                    and get_json_object(get_json_object(get_json_object(get_json_object(content, '$.data_json'), '$.voice_value'), '$.semantic_list[0]'), '$.command') = 'navigation/search'
                    and get_json_object(get_json_object(get_json_object(get_json_object(get_json_object(content, '$.data_json'), '$.voice_value'), '$.semantic_list[0]'), '$.content'), '$.target') = 'NAVI_NORMAL'
              ) t_31
                  join (
             select
                 vin,
                 create_time,
                 receive_time,
                 collect_time,
                 veh_model,
                 veh_config,
                 veh_level,
                 content,
                 event_key,
                 get_json_object(get_json_object(content, '$.data_json'), '$.record_id') AS record_id,
                 get_json_object(content, '$.target_time') as target_time,
                 dt
             from ods_ssp_cloud_uploader_track_all_offline_di_prod
             where dt = '{-dt}'
               and event_key = 'X_Map_001_0009'
               and get_json_object(get_json_object(content, '$.data_json'), '$.record_id') IS NOT NULL
         ) t_9 on t_31.record_id = t_9.record_id
                  join (
             select
                 vin,
                 create_time,
                 receive_time,
                 collect_time,
                 veh_model,
                 veh_config,
                 veh_level,
                 content,
                 event_key,
                 get_json_object(get_json_object(content, '$.data_json'), '$.record_id') AS record_id,
                 get_json_object(content, '$.target_time') as target_time,
                 dt
             from ods_ssp_cloud_uploader_track_all_offline_di_prod
             where dt = '{-dt}'
               and event_key = 'X_Map_001_0008'
               and get_json_object(get_json_object(content, '$.data_json'), '$.record_id') IS NOT NULL
         ) t_8 on t_31.record_id = t_8.record_id
                  join (
             select
                 vin,
                 create_time,
                 receive_time,
                 collect_time,
                 veh_model,
                 veh_config,
                 veh_level,
                 content,
                 event_key,
                 get_json_object(get_json_object(content, '$.data_json'), '$.record_id') AS record_id,
                 get_json_object(content, '$.target_time') as target_time,
                 dt
             from ods_ssp_cloud_uploader_track_all_offline_di_prod
             where dt = '{-dt}'
               and event_key = 'X_Map_009_0027'
               and get_json_object(get_json_object(content, '$.data_json'), '$.record_id') IS NOT NULL
         ) t_27 on t_31.record_id = t_27.record_id
                  join (
             select
                 vin,
                 create_time,
                 receive_time,
                 collect_time,
                 veh_model,
                 veh_config,
                 veh_level,
                 content,
                 event_key,
                 get_json_object(get_json_object(content, '$.data_json'), '$.record_id') AS record_id,
                 get_json_object(content, '$.target_time') as target_time,
                 dt
             from ods_ssp_cloud_uploader_track_all_offline_di_prod
             where dt = '{-dt}'
               and event_key = 'X_Map_009_0028'
               and get_json_object(get_json_object(content, '$.data_json'), '$.record_id') IS NOT NULL
         ) t_28 on t_31.record_id = t_28.record_id
                  join (
             select
                 vin,
                 create_time,
                 receive_time,
                 collect_time,
                 veh_model,
                 veh_config,
                 veh_level,
                 content,
                 event_key,
                 get_json_object(get_json_object(content, '$.data_json'), '$.record_id') AS record_id,
                 get_json_object(content, '$.target_time') as target_time,
                 dt
             from ods_ssp_cloud_uploader_track_all_offline_di_prod
             where dt = '{-dt}'
               and event_key = 'X_Map_001_0010'
               and get_json_object(get_json_object(content, '$.data_json'), '$.record_id') IS NOT NULL
         ) t_10 on t_31.record_id = t_10.record_id
                  left join (
             SELECT
                 vin,
                 is_user_vehicle,
                 ota_version,
                 year_model,
                 CASE WHEN substr(year_model, 1, 4) < '2024' THEN 'SS2' ELSE 'SS3' END AS hard_ver
             from dm_vom_vehvoice_basic_df
             where dt = '{-dt}'
         ) t_vom on t_31.vin = t_vom.vin
)





-----------------------------------------------------
select
    uuid() as uuid,
    now() as ts,
    dt,
    ota_version,
    hard_ver,
    X_Map_001_0009_time,
    X_Map_001_0008_time
from (
         select
             t_31.dt,
             ota_version,
             hard_ver,
             t_31.record_id,
             t_9.target_time as X_Map_001_0009_time,
             t_8.target_time as X_Map_001_0008_time
         from (
                  select
                      *,
                      get_json_object(get_json_object(content, '$.data_json'), '$.record_id') AS record_id
                  from ods_ssp_cloud_uploader_track_all_offline_di_prod
                  where dt = '{-dt}'
                    and event_key = 'vehvoice_002_0031'
                    and get_json_object(get_json_object(get_json_object(get_json_object(content, '$.data_json'), '$.voice_value'), '$.semantic_list[0]'), '$.command') = 'navigation/search'
                    and get_json_object(get_json_object(get_json_object(get_json_object(get_json_object(content, '$.data_json'), '$.voice_value'), '$.semantic_list[0]'), '$.content'), '$.target') = 'NAVI_NORMAL'
              ) t_31
                  join (
             select
                 vin,
                 create_time,
                 receive_time,
                 collect_time,
                 veh_model,
                 veh_config,
                 veh_level,
                 content,
                 event_key,
                 get_json_object(get_json_object(content, '$.data_json'), '$.record_id') AS record_id,
                 get_json_object(content, '$.target_time') as target_time,
                 dt
             from ods_ssp_cloud_uploader_track_all_offline_di_prod
             where dt = '{-dt}'
               and event_key = 'X_Map_001_0009'
               and get_json_object(get_json_object(content, '$.data_json'), '$.record_id') IS NOT NULL
         ) t_9 on t_31.record_id = t_9.record_id
                  join (
             select
                 vin,
                 create_time,
                 receive_time,
                 collect_time,
                 veh_model,
                 veh_config,
                 veh_level,
                 content,
                 event_key,
                 get_json_object(get_json_object(content, '$.data_json'), '$.record_id') AS record_id,
                 get_json_object(content, '$.target_time') as target_time,
                 dt
             from ods_ssp_cloud_uploader_track_all_offline_di_prod
             where dt = '{-dt}'
               and event_key = 'X_Map_001_0008'
               and get_json_object(get_json_object(content, '$.data_json'), '$.record_id') IS NOT NULL
         ) t_8 on t_31.record_id = t_8.record_id
                  left join (
             SELECT
                 vin,
                 is_user_vehicle,
                 ota_version,
                 year_model,
                 CASE WHEN substr(year_model, 1, 4) < '2024' THEN 'SS2' ELSE 'SS3' END AS hard_ver
             from dm_vom_vehvoice_basic_df
             where dt = '{-dt}'
         ) t_vom on t_31.vin = t_vom.vin
)




