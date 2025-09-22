# Optimized Presto queries with reduced columns (18 instead of 38)

query_test_optimized = '''
with cte_temp as (
  (
  select
    CAST(FROM_UNIXTIME(x.created_at / 1000) AS DATE) as exe_date,
    build_version,
    test_method as test_case,
    tr.lob as automation_lob,
    x.platform,
    x.framework_type,
    x.pipeline,
    x.app_name,
    CONCAT(UPPER(x.app_name), ' ', UPPER(x.platform)) as app_platform,
    x.os_version,
    x.result,
    x.state,
    x.failure_category_l1,
    x.failure_category_l1_triaged,
    x.failure_category_l2,
    x.ticket_id as bug_id,
    'TeRRA' as method_type,
    count(distinct x.run_uuid) as no_of_runs,
    count(
      distinct CASE
        WHEN x.result = 'PASSED'
        then x.run_uuid
      END
    ) as no_of_passed_runs,
    count(
      distinct case
        when lower(x.failure_category_l1) = 'infra_dl'
        or lower(x.failure_category_l1_triaged) = 'infra_dl'
        then x.run_uuid
      END
    ) AS dl_failures,
    count(
      distinct CASE
        WHEN lower(x.failure_category_l1) = 'infra'
        OR (
          x.failure_category_l1 = 'TC'
          and (
            lower(x.failure_category_l1_triaged) = 'infra'
          )
        )
        then x.run_uuid
      END
    ) as no_of_infra_runs,
    count(
      distinct CASE
        WHEN x.failure_category_l1 = 'TC'
        and (
          lower(x.failure_category_l1_triaged) = 'infra'
        )
        then x.run_uuid
      END
    ) as no_of_masked_infra_runs
  from
    rawdata_user.mysql_metesv2_metesv2_test_run_rows x
    join rawdata_user.mysql_metesv2_metesv2_test_execution_rows z on x.execution_uuid = z.execution_uuid
    left join kirby_external_data.terra_test_methods_1 tr on trim(x.test_method) = trim(tr.automation_name) 
  where
    is_final_retry_group = true
    and z.state!='aborted'
    and x.framework_type = 'appium'
    and z.trigger_by = 'v2-auto-trigger'
    and x.test_bundle_name in (
       'android_carbon_uwf_critical_appium_main',
      'ios_carbon_uwf_critical_appium_main',
      'ios_helix_dual_release_appium_main',
      'android_helix_dual_release_appium_main',
      'android_carbon_release_appium_main',
      'ios_carbon_release_appium_main',
      'ios_helix_dual_uwf_critical_appium_main',
      'android_helix_dual_uwf_critical_appium_main',
      'android_eats_release_appium_main',
      'ios_eats_release_appium_main',
      'android_eats_release_appium_main',
      'ios_eats_uwf_critical_appium_main',
      'android_eats_uwf_critical_appium_main',
      'ios_helix_dual_deeplinks_release',
      'android_helix_dual_deeplinks_release',
      'ios_eats_ai_release_appium_main',
       'ios-carbon-do-release-appium-main',
      'android-carbon-do-release-appium-main',
      'android-helix_dual-do-release-appium-main',
      'ios-helix_dual-do-release-appium-main',
      'ios_carbon_release_appium_main',
      'android_carbon_release_appium_main',
      'android_helix_dual_release_appium_main',
      'ios_helix_dual_release_appium_main',
'ios_carbon_nightly_appium_main',
      'android_carbon_nightly_appium_main',
      'android_helix_dual_nightly_appium_main',
      'ios_helix_dual_nightly_appium_main',
      'android carbon us&c run1 appium main',
      'android helix us&c run1 appium main',
      'ios carbon us&c run1 appium main',
      'ios helix us&c run1 appium main',
      'android-carbon-usnc-run2-appium-main',
      'ios_carbon_usnc_run2_appium_main',
      'android_helix_usnc_run2_appium_main',
      'ios_helix_usnc_run2_appium_main',
      'android_restaurants_release_appium_main',
'android_restaurants_nightly_appium_main'
    )
    and lower(failure_category_l2) != 'evicted' 
    and CAST(FROM_UNIXTIME(x.created_at / 1000) AS DATE) >= DATE '2025-01-01'


    and (build_version LIKE '%10000%' 
 OR build_version LIKE '%10001%' 
 OR build_version LIKE '%10002%' 
 OR build_version LIKE '%10003%' 
 OR build_version LIKE '%10004%' 
 OR build_version = '3.661.10001')
  GROUP BY
    1,
    2,
    3,
    4,
    5,
    6,
    7,
    8,
    9,
    10,
    11,
    12,
    13,
    14,
    15,
    16,
    17
  ORDER BY
    1
)
    
    UNION 
    
(
select
    CAST(FROM_UNIXTIME(x.created_at / 1000) AS DATE) as exe_date,
    build_version,
    test_method as test_case,
    automation_lob,
    x.platform,
    x.framework_type,
    x.pipeline,
    x.app_name,
    CONCAT(UPPER(x.app_name), ' ', UPPER(x.platform)) as app_platform,
    x.os_version,
    x.result,
    x.state,
    x.failure_category_l1,
    x.failure_category_l1_triaged,
    x.failure_category_l2,
    x.ticket_id as bug_id,
    'Non TeRRA' as method_type,
    count(distinct x.run_uuid) as no_of_runs,
    count(
      distinct CASE
        WHEN x.result = 'PASSED'
        then x.run_uuid
      END
    ) as no_of_passed_runs,
    count(
      distinct case
        when lower(x.failure_category_l1) = 'infra_dl'
        or lower(x.failure_category_l1_triaged) = 'infra_dl'
        then x.run_uuid
      END
    ) AS dl_failures,
    count(
      distinct CASE
        WHEN lower(x.failure_category_l1) = 'infra'
        OR (
          x.failure_category_l1 = 'TC'
          and (
            lower(x.failure_category_l1_triaged) = 'infra'
          )
        )
        then x.run_uuid
      END
    ) as no_of_infra_runs,
    count(
      distinct CASE
        WHEN x.failure_category_l1 = 'TC'
        and (
          lower(x.failure_category_l1_triaged) = 'infra'
        )
        then x.run_uuid
      END
    ) as no_of_masked_infra_runs
  from
    rawdata_user.mysql_metesv2_metesv2_test_run_rows x
    join rawdata_user.mysql_metesv2_metesv2_test_execution_rows z on x.execution_uuid = z.execution_uuid
    left join core_automation_platform.test_case tbl2 on trim(x.test_method) = trim(tbl2.appiumautomationname)
    and lower(x.platform) = lower(tbl2.platform)
    left join kirby_external_data.mta_features af on tbl2.scenarioname = af.feature
  where
    is_final_retry_group = true
    and z.state!='aborted'
    and x.framework_type = 'appium'
    and z.trigger_by = 'v2-auto-trigger'
    and x.test_bundle_name in (
       'android_carbon_uwf_critical_appium_main',
      'ios_carbon_uwf_critical_appium_main',
      'ios_helix_dual_release_appium_main',
      'android_helix_dual_release_appium_main',
      'android_carbon_release_appium_main',
      'ios_carbon_release_appium_main',
      'ios_helix_dual_uwf_critical_appium_main',
      'android_helix_dual_uwf_critical_appium_main',
      'android_eats_release_appium_main',
      'ios_eats_release_appium_main',
      'android_eats_release_appium_main',
      'ios_eats_uwf_critical_appium_main',
      'android_eats_uwf_critical_appium_main',
      'ios_helix_dual_deeplinks_release',
      'android_helix_dual_deeplinks_release',
      'ios_eats_ai_release_appium_main',
       'ios-carbon-do-release-appium-main',
      'android-carbon-do-release-appium-main',
      'android-helix_dual-do-release-appium-main',
      'ios-helix_dual-do-release-appium-main',
      'ios_carbon_release_appium_main',
      'android_carbon_release_appium_main',
      'android_helix_dual_release_appium_main',
      'ios_helix_dual_release_appium_main',
'ios_carbon_nightly_appium_main',
      'android_carbon_nightly_appium_main',
      'android_helix_dual_nightly_appium_main',
      'ios_helix_dual_nightly_appium_main',
      'android carbon us&c run1 appium main',
      'android helix us&c run1 appium main',
      'ios carbon us&c run1 appium main',
      'ios helix us&c run1 appium main',
      'android-carbon-usnc-run2-appium-main',
      'ios_carbon_usnc_run2_appium_main',
      'android_helix_usnc_run2_appium_main',
      'ios_helix_usnc_run2_appium_main',
      'android_restaurants_release_appium_main',
'android_restaurants_nightly_appium_main'
    )
    and lower(failure_category_l2) != 'evicted' 
    and CAST(FROM_UNIXTIME(x.created_at / 1000) AS DATE) >= DATE '2025-01-01'
     
    and (build_version LIKE '%10000%' 
 OR build_version LIKE '%10001%' 
 OR build_version LIKE '%10002%' 
 OR build_version LIKE '%10003%' 
 OR build_version LIKE '%10004%' 
 OR build_version = '3.661.10001')
    AND date(hadoop_datestr) >= CURRENT_DATE - INTERVAL '4' DAY
  GROUP BY
    1,
    2,
    3,
    4,
    5,
    6,
    7,
    8,
    9,
    10,
    11,
    12,
    13,
    14,
    15,
    16,
    17
  ORDER BY
    1
)
),
cte_union as (
  select
    tbl1.*,
    CASE
      WHEN tbl2.issue_id IS NULL
      THEN tbl3.issue_id
      ELSE tbl2.issue_id
    END AS issue_id
  from
    cte_temp tbl1
    left join devtools.jira_issue_changelog tbl2 on upper(trim(tbl1.bug_id)) = trim(tbl2.old_value)
    and tbl2.field_name = 'Key'
    left join devtools.jira_issues tbl3 on upper(trim(tbl1.bug_id)) = trim(tbl3.issue_key)
),
bugs_data as (
  select
    *,
    labels_str as lbl,
    case
      when (
        labels_str like '%#E2EProductBug%'
        or labels_str like '%E2EUserWorkFlowProductBug%'
      )
      and labels_str like '%E2EAutomation%'
      then 1
      else 0
    end as product_bugs_captured,
    case
      when (
        labels_str like '%E2EPotentialPB%'
      )
      and labels_str like '%E2EAutomation%'
      then 1
      else 0
    end as potential_product_bugs_captured,
    case
      when (
        labels_str like '%E2EProductFlowChange%'
        or labels_str like '%E2EUserWorkFlowProductFlowChange%'
        or labels_str like '%#E2EMetesDelay%'
      )
      and labels_str like '%E2EAutomation%'
      then 1
      else 0
    end as product_flow_changes,
    case
      when labels_str like '%E2ETestScript%'
      and labels_str like '%E2EAutomation%'
      then 1
      else 0
    end as test_script_bugs,
    case
      when labels_str like '%#E2EFramework%'
      then 1
      else 0
    end as framework_tickets,
    case
      when labels_str like '%E2EMaskedInfra%'
      and labels_str like '%E2EAutomation%'
      then 1
      else 0
    end as masked_infra_bugs,
    case
      when (
        labels_str like '%E2EStudio%'
        or labels_str like '%E2EDeviceinfra%'
        or labels_str like '%E2E-EmulatorInfra%'
        or labels_str like '%E2ETestInfra%'
        or labels_str like '%Needs-CAP-Attention%'
      )
      and labels_str like '%E2EAutomation%'
      then 1
      else 0
    end as Infra_bugs
  from
    (
      select
        *,
        ARRAY_JOIN(labels, ',') as labels_str
      from
        devtools.jira_issues
    ) jira_master
  where
    labels_str like '%E2EProductBug%'
    or labels_str like '%E2EProductFlowChange%'
    or labels_str like '%E2ETestScript%'
    or labels_str like '%E2EStudio%'
    or labels_str like '%E2EDeviceinfra%'
    or labels_str like '%E2E-EmulatorInfra%'
    or labels_str like '%E2ETestInfra%'
    or labels_str like '%E2EUserFunnel%'
    or labels_str like '%E2EWeb%'
    or labels_str like '%E2ELocalization%'
    or labels_str like '%E2ELocalizationOnly%'
    or labels_str like '%E2EUserWorkFlow%'
    or labels_str like '%E2EUserWorkFlowProductBug%'
    or labels_str like '%E2EUserWorkFlowProductFlowChange%'
    or labels_str like '%#E2EMetesDelay%'
    or labels_str like '%Needs-CAP-Attention%'
    or labels_str like '%E2EAutomation%'
    or labels_str like '%#E2EOutOfRotation%'
    or labels_str like '%#E2EMaskedInfra%'
    or labels_str like '%#E2EFramework%'
    or labels_str like '%E2EProductSafetyAndInsuranceSpecific%'
),
final as (
select
  p.*,
  DENSE_RANK() over (
        partition by app_platform,
        automation_lob
        order by
          exe_date desc
      ) AS rnk,
  q.issue_key,status,priority,
  q.created as bug_creation_date,
  q.resolution_date as bug_resolution_date,
  jp.project_name,
  jp.project_category_name,
      IF(
        q.status = 'Closed',
        DATE_DIFF('DAY', q.created, q.resolution_date),
        DATE_DIFF('DAY', q.created, CURRENT_DATE)
      ) as age,
  case
    when product_bugs_captured = 1
    and no_of_infra_runs = 0
    then 'Product Bugs'
    when potential_product_bugs_captured = 1
    and no_of_infra_runs = 0
    then 'Potential Product Bugs'
    when product_flow_changes = 1
    and no_of_infra_runs = 0
    then 'Product Flow Changes'
    when test_script_bugs = 1
    and no_of_infra_runs = 0
    and lower(q.issue_type) in ('bug')
    then 'Test Script Failures'
    when masked_infra_bugs = 1
    then 'Masked Infra'
    when framework_tickets = 1
    then 'Framework Tickets'
    when result = 'FAILED'
    and (
      bug_id IS NULL
      or bug_id = ''
    )
    and no_of_infra_runs = 0
    THEN 'Not triaged'
    when result = 'FAILED'
    and bug_id IS NOT NULL
    and bug_id != ''
    and no_of_infra_runs = 0
    and Infra_bugs = 0
    then 'Others'
    else 'NA'
  end as bug_type,
  case
    when product_bugs_captured = 1
    and lbl like '%#E2EAlways%'
    then 'Always'
    when product_bugs_captured = 1
    and lbl like '%#E2EIntermittent%'
    then 'Intermittent'
    when product_bugs_captured = 1
    and lbl like '%#E2EOneTime%'
    then 'One time'
    else 'NA'
  end as pb_bug_type,
  case
    when product_flow_changes = 1
    and lbl like '%#E2EPFC_Flowchange%'
    then 'Flow Change'
    when product_flow_changes = 1
    and lbl like '%#E2EPFC_Identifier%'
    then 'Identifier'
    when product_flow_changes = 1
    and lbl not like '%#E2EPFC_Flowchange%'
    and lbl not like '%#E2EPFC_Identifier%'
    then 'Others'
    else 'NA'
  end as pfc_bug_type,
  case
    when product_bugs_captured = 1
    and lbl like '%#E2EAlways%'
    then 'Always'
    when product_bugs_captured = 1
    and lbl like '%#E2EIntermittent%'
    then 'Intermittent'
    when product_bugs_captured = 1
    and lbl like '%#E2EOneTime%'
    then 'One time'
    when product_flow_changes = 1
    and lbl like '%#E2EAlways%'
    then 'Always'
    when product_flow_changes = 1
    and lbl like '%#E2EIntermittent%'
    then 'Intermittent'
    when product_flow_changes = 1
    and lbl like '%#E2EOneTime%'
    then 'One time'
    else ''
  end as occurence,
  r.no_of_platforms
from
  cte_union p
  left join bugs_data q on p.issue_id = q.issue_id
  left join devtools.jira_projects jp on q.project_id = jp.project_id
  left join kirby_external_data.automation_bug_platform_count_mapping r on q.issue_key = r.issue_key
  left join kirby_external_data.uber_holidays uh on p.exe_date = date(uh.holiday_date)
where
  uh.holiday_date IS NULL
  and p.test_case not in (
    select
      test_method
    from
      kirby_external_data.excluded_test_methods
  )

  -- and automation_lob not in ('GMP','Localization')
  and automation_lob not in ('Localization'))
  
select * , case
when bug_type='NA' and pb_bug_type !='NA' then 'Product Bugs'
when bug_type='NA' and pfc_bug_type !='NA' then 'Product Flow Changes'
when (failure_category_l1='INFRA' or failure_category_l1='infra') and lower(failure_category_l1)!='infra_dl' and lower(failure_category_l1_triaged)!='infra_dl' then 'Infra'
when (failure_category_l1='INFRA' or failure_category_l1='infra') then 'Infra'
when lower(failure_category_l1)='tc' and lower(failure_category_l1_triaged)='infra' then 'Masked Infra'
when failure_category_l1='TC' AND (failure_category_l1_triaged='INFRA' or lower(failure_category_l1_triaged)='infra') and pb_bug_type ='NA' and pfc_bug_type = 'NA' then 'Masked Infra'
when bug_type='Product Bugs' then 'Product Bugs'
when bug_type='Product Flow Changes' then 'Product Flow Changes'
when bug_type='Potential Product Bugs' then 'Potential Product Bugs'
when bug_type='Test Script Failures' then 'Test Script Failures'
when bug_type='Framework Tickets' then 'Framework Tickets' 
when bug_type='Others' and lower(failure_category_l1)!='infra_dl' and lower(failure_category_l1_triaged)!='infra_dl' then 'Unclassified'
when bug_type='Others' and lower(failure_category_l1)='tc' then 'Unclassified'
when bug_type='NA' and lower(failure_category_l1)='tc' and lower(failure_category_l1_triaged)='' then 'Unclassified'
when bug_type='Masked Infra' and lower(failure_category_l1)='tc' and lower(failure_category_l1_triaged)!='infra' and lower(failure_category_l1_triaged)!='infra_dl' then 'Unclassified'
when lower(failure_category_l1)='infra_dl' or lower(failure_category_l1_triaged)='infra_dl'  then 'DL Failures'
when lower(failure_category_l1)='tc' and lower(failure_category_l1_triaged)='infra_dl'  then 'DL Failures'
when result='FAILED' then 'Unclassified'
else 'NA'
end as l1_category from final '''


query_feature_optimized = '''
with cte_temp as (
  (
  select
    CAST(FROM_UNIXTIME(x.created_at / 1000) AS DATE) as exe_date,
    build_version,
    scenario_name as scenarioname,
    tr.lob as automation_lob,
    x.platform,
    x.framework_type,
    x.pipeline,
    x.app_name,
    CONCAT(UPPER(x.app_name), ' ', UPPER(x.platform)) as app_platform,
    x.os_version,
    x.result,
    x.state,
    x.failure_category_l1,
    x.failure_category_l1_triaged,
    x.failure_category_l2,
    x.ticket_id as bug_id,
    'TeRRA' as method_type,
    count(distinct x.run_uuid) as no_of_runs,
    count(
      distinct CASE
        WHEN x.result = 'PASSED'
        then x.run_uuid
      END
    ) as no_of_passed_runs,
    count(
      distinct case
        when lower(x.failure_category_l1) = 'infra_dl'
        or lower(x.failure_category_l1_triaged) = 'infra_dl'
        then x.run_uuid
      END
    ) AS dl_failures,
    count(
      distinct CASE
        WHEN lower(x.failure_category_l1) = 'infra'
        OR (
          x.failure_category_l1 = 'TC'
          and (
            lower(x.failure_category_l1_triaged) = 'infra'
          )
        )
        then x.run_uuid
      END
    ) as no_of_infra_runs,
    count(
      distinct CASE
        WHEN x.failure_category_l1 = 'TC'
        and (
          lower(x.failure_category_l1_triaged) = 'infra'
        )
        then x.run_uuid
      END
    ) as no_of_masked_infra_runs
  from
    rawdata_user.mysql_metesv2_metesv2_test_run_rows x
    join rawdata_user.mysql_metesv2_metesv2_test_execution_rows z on x.execution_uuid = z.execution_uuid
    left join kirby_external_data.terra_test_methods_1 tr on trim(x.test_method) = trim(tr.automation_name)
  where
    is_final_retry_group = true
    and x.framework_type = 'appium'
    and z.state!='aborted'
    and z.trigger_by = 'v2-auto-trigger'
    and x.test_bundle_name in (
      'android_carbon_uwf_critical_appium_main',
      'ios_carbon_uwf_critical_appium_main',
      'ios_helix_dual_release_appium_main',
      'android_helix_dual_release_appium_main',
      'android_carbon_release_appium_main',
      'ios_carbon_release_appium_main',
      'ios_helix_dual_uwf_critical_appium_main',
      'android_helix_dual_uwf_critical_appium_main',
      'android_eats_release_appium_main',
      'ios_eats_release_appium_main',
      'android_eats_release_appium_main',
      'ios_eats_uwf_critical_appium_main',
      'android_eats_uwf_critical_appium_main',
      'ios_helix_dual_deeplinks_release',
      'android_helix_dual_deeplinks_release',
      'ios_eats_ai_release_appium_main',
       'ios-carbon-do-release-appium-main',
      'android-carbon-do-release-appium-main',
      'android-helix_dual-do-release-appium-main',
      'ios-helix_dual-do-release-appium-main',
      'ios_carbon_release_appium_main',
      'android_carbon_release_appium_main',
      'android_helix_dual_release_appium_main',
      'ios_helix_dual_release_appium_main',
      'ios_carbon_nightly_appium_main',
      'android_carbon_nightly_appium_main',
      'android_helix_dual_nightly_appium_main',
      'ios_helix_dual_nightly_appium_main',
      'android carbon us&c run1 appium main',
      'android helix us&c run1 appium main',
      'ios carbon us&c run1 appium main',
      'ios helix us&c run1 appium main',
      'android-carbon-usnc-run2-appium-main',
      'ios_carbon_usnc_run2_appium_main',
      'android_helix_usnc_run2_appium_main',
      'ios_helix_usnc_run2_appium_main',
      'android_restaurants_release_appium_main',
'android_restaurants_nightly_appium_main'
    )
    and lower(failure_category_l2) != 'evicted'
    and CAST(FROM_UNIXTIME(x.created_at / 1000) AS DATE) >= DATE '2025-01-01'
    
    and (build_version LIKE '%10000%' 
 OR build_version LIKE '%10001%' 
 OR build_version LIKE '%10002%' 
 OR build_version LIKE '%10003%' 
 OR build_version LIKE '%10004%' 
 OR build_version = '3.661.10001')

    and x.test_method not in (
    select
      test_method
    from
      kirby_external_data.excluded_test_methods
  )
  GROUP BY
    1,
    2,
    3,
    4,
    5,
    6,
    7,
    8,
    9,
    10,
    11,
    12,
    13,
    14,
    15,
    16,
    17
  ORDER BY
    1
)
    
    UNION 
    
(
select
    CAST(FROM_UNIXTIME(x.created_at / 1000) AS DATE) as exe_date,
    build_version,
    scenarioname,
    automation_lob,
    x.platform,
    x.framework_type,
    x.pipeline,
    x.app_name,
    CONCAT(UPPER(x.app_name), ' ', UPPER(x.platform)) as app_platform,
    x.os_version,
    x.result,
    x.state,
    x.failure_category_l1,
    x.failure_category_l1_triaged,
    x.failure_category_l2,
    x.ticket_id as bug_id,
    'Non TeRRA' as method_type,
    count(distinct x.run_uuid) as no_of_runs,
    count(
      distinct CASE
        WHEN x.result = 'PASSED'
        then x.run_uuid
      END
    ) as no_of_passed_runs,
    count(
      distinct case
        when lower(x.failure_category_l1) = 'infra_dl'
        or lower(x.failure_category_l1_triaged) = 'infra_dl'
        then x.run_uuid
      END
    ) AS dl_failures,
    count(
      distinct CASE
        WHEN lower(x.failure_category_l1) = 'infra'
        OR (
          x.failure_category_l1 = 'TC'
          and (
            lower(x.failure_category_l1_triaged) = 'infra'
          )
        )
        then x.run_uuid
      END
    ) as no_of_infra_runs,
    count(
      distinct CASE
        WHEN x.failure_category_l1 = 'TC'
        and (
          lower(x.failure_category_l1_triaged) = 'infra'
        )
        then x.run_uuid
      END
    ) as no_of_masked_infra_runs
  from
    rawdata_user.mysql_metesv2_metesv2_test_run_rows x
    join rawdata_user.mysql_metesv2_metesv2_test_execution_rows z on x.execution_uuid = z.execution_uuid
    left join core_automation_platform.test_case tbl2 on trim(x.test_method) = trim(tbl2.appiumautomationname)
    and lower(x.platform) = lower(tbl2.platform)
    left join kirby_external_data.mta_features af on tbl2.scenarioname = af.feature
  where
    is_final_retry_group = true
    and x.framework_type = 'appium'
    and z.state!='aborted'
    and z.trigger_by = 'v2-auto-trigger'
    and x.test_bundle_name in (
      'android_carbon_uwf_critical_appium_main',
      'ios_carbon_uwf_critical_appium_main',
      'ios_helix_dual_release_appium_main',
      'android_helix_dual_release_appium_main',
      'android_carbon_release_appium_main',
      'ios_carbon_release_appium_main',
      'ios_helix_dual_uwf_critical_appium_main',
      'android_helix_dual_uwf_critical_appium_main',
      'android_eats_release_appium_main',
      'ios_eats_release_appium_main',
      'android_eats_release_appium_main',
      'ios_eats_uwf_critical_appium_main',
      'android_eats_uwf_critical_appium_main',
      'ios_helix_dual_deeplinks_release',
      'android_helix_dual_deeplinks_release',
      'ios_eats_ai_release_appium_main',
       'ios-carbon-do-release-appium-main',
      'android-carbon-do-release-appium-main',
      'android-helix_dual-do-release-appium-main',
      'ios-helix_dual-do-release-appium-main',
      'ios_carbon_release_appium_main',
      'android_carbon_release_appium_main',
      'android_helix_dual_release_appium_main',
      'ios_helix_dual_release_appium_main',
      'ios_carbon_nightly_appium_main',
      'android_carbon_nightly_appium_main',
      'android_helix_dual_nightly_appium_main',
      'ios_helix_dual_nightly_appium_main',
      'android carbon us&c run1 appium main',
      'android helix us&c run1 appium main',
      'ios carbon us&c run1 appium main',
      'ios helix us&c run1 appium main',
       'android-carbon-usnc-run2-appium-main',
      'ios_carbon_usnc_run2_appium_main',
      'android_helix_usnc_run2_appium_main',
      'ios_helix_usnc_run2_appium_main',
      'android_restaurants_release_appium_main',
'android_restaurants_nightly_appium_main'
    )
    and lower(failure_category_l2) != 'evicted'
    and CAST(FROM_UNIXTIME(x.created_at / 1000) AS DATE) >= DATE '2025-01-01'
   
    and (build_version LIKE '%10000%' 
 OR build_version LIKE '%10001%' 
 OR build_version LIKE '%10002%' 
 OR build_version LIKE '%10003%' 
 OR build_version LIKE '%10004%' 
 OR build_version = '3.661.10001')

    and x.test_method not in (
    select
      test_method
    from
      kirby_external_data.excluded_test_methods
  )
    AND date(hadoop_datestr) >= CURRENT_DATE - INTERVAL '4' DAY
  GROUP BY
    1,
    2,
    3,
    4,
    5,
    6,
    7,
    8,
    9,
    10,
    11,
    12,
    13,
    14,
    15,
    16,
    17
  ORDER BY
    1
)
),
cte_union as (
  select
    tbl1.*,
    CASE
      WHEN tbl2.issue_id IS NULL
      THEN tbl3.issue_id
      ELSE tbl2.issue_id
    END AS issue_id
  from
    cte_temp tbl1
    left join devtools.jira_issue_changelog tbl2 on upper(trim(tbl1.bug_id)) = trim(tbl2.old_value)
    and tbl2.field_name = 'Key'
    left join devtools.jira_issues tbl3 on upper(trim(tbl1.bug_id)) = trim(tbl3.issue_key)
),
bugs_data as (
  select
    *,
    labels_str as lbl,
    case
      when (
        labels_str like '%#E2EProductBug%'
        or labels_str like '%E2EUserWorkFlowProductBug%'
      )
      and labels_str like '%E2EAutomation%'
      then 1
      else 0
    end as product_bugs_captured,
    case
      when (
        labels_str like '%E2EPotentialPB%'
      )
      and labels_str like '%E2EAutomation%'
      then 1
      else 0
    end as potential_product_bugs_captured,
    case
      when (
        labels_str like '%E2EProductFlowChange%'
        or labels_str like '%E2EUserWorkFlowProductFlowChange%'
        or labels_str like '%#E2EMetesDelay%'
      )
      and labels_str like '%E2EAutomation%'
      then 1
      else 0
    end as product_flow_changes,
    case
      when labels_str like '%E2ETestScript%'
      and labels_str like '%E2EAutomation%'
      then 1
      else 0
    end as test_script_bugs,
    case
      when labels_str like '%#E2EFramework%'
      then 1
      else 0
    end as framework_tickets,
    case
      when labels_str like '%E2EMaskedInfra%'
      and labels_str like '%E2EAutomation%'
      then 1
      else 0
    end as masked_infra_bugs,
    case
      when (
        labels_str like '%E2EStudio%'
        or labels_str like '%E2EDeviceinfra%'
        or labels_str like '%E2E-EmulatorInfra%'
        or labels_str like '%E2ETestInfra%'
        or labels_str like '%Needs-CAP-Attention%'
      )
      and labels_str like '%E2EAutomation%'
      then 1
      else 0
    end as Infra_bugs
  from
    (
      select
        *,
        ARRAY_JOIN(labels, ',') as labels_str
      from
        devtools.jira_issues
    ) jira_master
  where
    labels_str like '%E2EProductBug%'
    or labels_str like '%E2EProductFlowChange%'
    or labels_str like '%E2ETestScript%'
    or labels_str like '%E2EStudio%'
    or labels_str like '%E2EDeviceinfra%'
    or labels_str like '%E2E-EmulatorInfra%'
    or labels_str like '%E2ETestInfra%'
    or labels_str like '%E2EUserFunnel%'
    or labels_str like '%E2EWeb%'
    or labels_str like '%E2ELocalization%'
    or labels_str like '%E2ELocalizationOnly%'
    or labels_str like '%E2EUserWorkFlow%'
    or labels_str like '%E2EUserWorkFlowProductBug%'
    or labels_str like '%E2EUserWorkFlowProductFlowChange%'
    or labels_str like '%#E2EMetesDelay%'
    or labels_str like '%Needs-CAP-Attention%'
    or labels_str like '%E2EAutomation%'
    or labels_str like '%#E2EOutOfRotation%'
    or labels_str like '%#E2EMaskedInfra%'
    or labels_str like '%#E2EFramework%'
    or labels_str like '%E2EProductSafetyAndInsuranceSpecific%'
),

final as (
select
  p.*,
  DENSE_RANK() over (
        partition by app_platform,
        automation_lob
        order by
          exe_date desc
      ) AS rnk,
  q.issue_key,status,priority,
  q.created as bug_creation_date,
  q.resolution_date as bug_resolution_date,
  jp.project_name,
  jp.project_category_name,
      IF(
        q.status = 'Closed',
        DATE_DIFF('DAY', q.created, q.resolution_date),
        DATE_DIFF('DAY', q.created, CURRENT_DATE)
      ) as age,
  case
    when product_bugs_captured = 1
    and no_of_infra_runs = 0
    then 'Product Bugs'
    when potential_product_bugs_captured = 1
    and no_of_infra_runs = 0
    then 'Potential Product Bugs'
    when product_flow_changes = 1
    and no_of_infra_runs = 0
    then 'Product Flow Changes'
    when test_script_bugs = 1
    and no_of_infra_runs = 0
    and lower(q.issue_type) in ('bug')
    then 'Test Script Failures'
    when masked_infra_bugs = 1
    then 'Masked Infra'
    when framework_tickets = 1
    then 'Framework Tickets'
    when result = 'FAILED'
    and (
      bug_id IS NULL
      or bug_id = ''
    )
    and no_of_infra_runs = 0
    THEN 'Not triaged'
    when result = 'FAILED'
    and bug_id IS NOT NULL
    and bug_id != ''
    and no_of_infra_runs = 0
    and Infra_bugs = 0
    then 'Others'
    else 'NA'
  end as bug_type,
  case
    when product_bugs_captured = 1
    and lbl like '%#E2EAlways%'
    then 'Always'
    when product_bugs_captured = 1
    and lbl like '%#E2EIntermittent%'
    then 'Intermittent'
    when product_bugs_captured = 1
    and lbl like '%#E2EOneTime%'
    then 'One time'
    else 'NA'
  end as pb_bug_type,
  case
    when product_flow_changes = 1
    and lbl like '%#E2EPFC_Flowchange%'
    then 'Flow Change'
    when product_flow_changes = 1
    and lbl like '%#E2EPFC_Identifier%'
    then 'Identifier'
    when product_flow_changes = 1
    and lbl not like '%#E2EPFC_Flowchange%'
    and lbl not like '%#E2EPFC_Identifier%'
    then 'Others'
    else 'NA'
  end as pfc_bug_type,
  case
    when product_bugs_captured = 1
    and lbl like '%#E2EAlways%'
    then 'Always'
    when product_bugs_captured = 1
    and lbl like '%#E2EIntermittent%'
    then 'Intermittent'
    when product_bugs_captured = 1
    and lbl like '%#E2EOneTime%'
    then 'One time'
    when product_flow_changes = 1
    and lbl like '%#E2EAlways%'
    then 'Always'
    when product_flow_changes = 1
    and lbl like '%#E2EIntermittent%'
    then 'Intermittent'
    when product_flow_changes = 1
    and lbl like '%#E2EOneTime%'
    then 'One time'
    else ''
  end as occurence,
  r.no_of_platforms
from
  cte_union p
  left join bugs_data q on p.issue_id = q.issue_id
  left join devtools.jira_projects jp on q.project_id = jp.project_id
  left join kirby_external_data.automation_bug_platform_count_mapping r on q.issue_key = r.issue_key
  left join kirby_external_data.uber_holidays uh on p.exe_date = date(uh.holiday_date)
where
  uh.holiday_date IS NULL
  -- and automation_lob not in ('GMP','Localization')
  and automation_lob not in ('Localization'))

  select * , case
when bug_type='NA' and pb_bug_type !='NA' then 'Product Bugs'
when bug_type='NA' and pfc_bug_type !='NA' then 'Product Flow Changes'
when (failure_category_l1='INFRA' or failure_category_l1='infra') and lower(failure_category_l1)!='infra_dl' and lower(failure_category_l1_triaged)!='infra_dl' then 'Infra'
when (failure_category_l1='INFRA' or failure_category_l1='infra') then 'Infra'
when lower(failure_category_l1)='tc' and lower(failure_category_l1_triaged)='infra' then 'Masked Infra'
when failure_category_l1='TC' AND (failure_category_l1_triaged='INFRA' or lower(failure_category_l1_triaged)='infra') and pb_bug_type ='NA' and pfc_bug_type = 'NA' then 'Masked Infra'
when bug_type='Product Bugs' then 'Product Bugs'
when bug_type='Product Flow Changes' then 'Product Flow Changes'
when bug_type='Potential Product Bugs' then 'Potential Product Bugs'
when bug_type='Test Script Failures' then 'Test Script Failures'
when bug_type='Framework Tickets' then 'Framework Tickets' 
when bug_type='Others' and lower(failure_category_l1)!='infra_dl' and lower(failure_category_l1_triaged)!='infra_dl' then 'Unclassified'
when bug_type='Others' and lower(failure_category_l1)='tc' then 'Unclassified'
when bug_type='NA' and lower(failure_category_l1)='tc' and lower(failure_category_l1_triaged)='' then 'Unclassified'
when bug_type='Masked Infra' and lower(failure_category_l1)='tc' and lower(failure_category_l1_triaged)!='infra' and lower(failure_category_l1_triaged)!='infra_dl' then 'Unclassified'
when lower(failure_category_l1)='infra_dl' or lower(failure_category_l1_triaged)='infra_dl'  then 'DL Failures'
when lower(failure_category_l1)='tc' and lower(failure_category_l1_triaged)='infra_dl'  then 'DL Failures'
when result='FAILED' then 'Unclassified'
else 'NA'
end as l1_category from final '''
