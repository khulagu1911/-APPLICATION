create or replace package body userbox_risk_mis.PKG_APPLICATION_SAS is

procedure dev_application_incr
is 
begin
  --userbox_risk_mis.pkg_log.step_begin;
  execute immediate 'truncate table userbox_risk_mis.dev_application_incr';
  
  insert /*+ parallel(12) */ into userbox_risk_mis.dev_application_incr
  select /*+ parallel(12) */
         ap.APPLICATION_KEY,
         ap.APPLICATION_NUM,
         ap.APPLICATION_CODE,
         ap.APPCHNNLTYPE_ID,
         ap.REGPOS_KEY,
         ap.REGPOS_ID,
         ap.POS_KEY,
         ap.POS_ID,
         ap.USDEXRATE_NVAL,
         ap.EUROEXRATE_NVAL,
         ap.SALESMAN_NAME,
         ap.ISOFFLINE_IND,
         ap.POSAUTH_CVAL,
         ap.REPORT_DELIVERY_ADDRESS_NAME,
         ap.BRANCH_NUM,
         ap.SYSTEM_OPEN_DT,
         ap.CQUEUESTATE_NAME,----------------------------
         ap.STATUS_DT,
         ap.STATUS_17_IND,
         ap.CONTRACT_KEY,
         ap.CONTRACT_NUM,
         ap.CONTRACT_SOURCE_SYSTEM_KEY,
         ap.ABSCONTRACT_NUM,
         ap.CREDITCARD_NUM,
         ap.PRODUCT_KEY,
         ap.CONF_PROD_CODE,
         ap.REQ_PROD_CODE,
         ap.PURCHASEAMOUNT_MAX_NVAL,
         ap.PDSGOODSGROUP_MAX_NAME,
         ap.PDSGOODSCAT_MAX_NAME,
         ap.PURCHASEAMOUNT_SUM_NVAL,
         ap.PDSNBRUNITS_SUM_CNT,
         ap.INSURLIFE_AMT,
         ap.INSURUNEMPLOYM_AMT,
         ap.REGION_MERCHANT_NAME,
         ap.REGION_KEY,
         ap.RISK_APPROVE_IND,
         ap.CAPP_PRODUCTGROUP_NAME,
         ap.PORTFOLIO_KEY,
         ap.PORTFOLIO_NUM,
         ap.SEGMENT_NUM,
         ap.SUB_SEGMENT_NUM,
         ap.CHANNEL_KEY,
         ap.SUB_CHANNEL_KEY,
         ap.ONLINE_FLAG_IND,
         ap.APPRISKSEGMENT_CVAL,
         ap.APPFRAUDFLAGTYPE_NAME,
         ap.APP_BASE_KEY,
         ap.APP_BASE_CODE,
         ap.SCORE_NVAL,
         ap.RELOAD_STATUS_CVAL,
         ap.DECISION_CVAL,
         ap.PURCHASE_LIST_TXT,
         ap.OFFER_CARD_IND,
         ap.ISS_CARD_IND,
         ap.CONSUMER_REJ_REASONS_NVAL,
         ap.REPRESENTATIVE_CVAL,  
         ap.CONTACT_DENY_IND,
         ap.DELIVERY_TYPE_NAME,
         ap.DELIVERY_MCO_TYPE_NAME,
         ap.CDA_IND,
         ap.AMOUNT_NVAL,
         ap.REQUEST_AMOUNT_NVAL,
         ap.RATE_NVAL,
         ap.REQUESTED_RATE_NVAL,
         ap.REQUEST_TERM_NVAL,
         ap.OFFERED_TERM_NVAL,
         ap.CURRENCY_NAME,
         ap.ASKED_TERM_NVAL,
         ap.REQUEST_START_PAY_NVAL,
         ap.START_PAY_NVAL,
         ap.OPERATOR_LOGIN_NAME,
         ap.PURPOSE_NAME,
         ap.DECISION_NVAL,---------------
         ap.DECISION_DT,
         ap.REASON_CVAL,
         ap.SUBREASON_01_CVAL,
         ap.CREDIT_LIMIT_AMT,
         ap.RATE_PERCENT_NVAL,
         ap.POSID_NAME,
         ap.OFFLINE_IND,
         ap.CHAIN_NAME,
         ap.NETWORK_NAME,
         ap.SMS_INFORM_IND,
         ap.RECEIVE_CHANNEL_NAME,
         ap.MARKETING_CHANNEL_KEY,
         ap.DIVISION_NAME,
         ap.ADDRESS_TXT,
         ap.PHONE_CVAL,
         ap.CREDITLIMIT_AMT,
         ap.CREDIT_CARD_ACTIVATED_IND,
         ap.GP_OFFER_IND,
         ap.CLIENT_DENY_TXT,
         ap.LOGIN_17_NAME,
         ap.APPROVE_IND,
         ap.DECISION_IND,
         ap.REASON_KEY,
         ap.INS_FRAUD_AMT,
         ap.CRED_INS_AMT,
         ap.CRED_INS_TERM_AMT,
         ap.PLT_AMT,
         ap.MERCHANT_INCENTIVE_RATE_NVAL,
         ap.MERCHANT_INCENTIVE_FIX_AMT,
         ap.LIFE_INSURANCE_AMT,
         ap.LIFE_AGREE_IND,
         ap.ILOE_INSURANCE_AMT,
         ap.ILOE_AGREE_IND,
         ap.TO_MERCHANT_AMT,
         ap.AGENT_NUM,
         ap.INSURANCE_COMPANY_NUM,
         ap.DWH_INS_DT,
         ap.DWH_UPD_DT,
         ap.DWH_PROCESS_KEY,
         ap.DWH_CRC_NVAL,
         ap.REG_COUNTRY_CODE,
         ap.CANCEL_REASON_CVAL,
         ap.VERIFICATION_CNT,
         ap.VALID_LOGIN_NAME,
         ap.VALID_DT,
         ap.CLOSE_COMMENT_TXT,
         ap.ITERATION_NUM,
         ap.MFO_IND,
         ap.LOCALS_AGENT_NUM,
         ap.REPORT_DELIVERY_ADDRESS_NUM,
         ap.PURPOSE_NUM,
         ap.APPFRAUDFLAGTYPE_NUM,
         ap.PDSGOODSCAT_MAX_NUM,
         ap.PDSGOODSGROUP_MAX_NUM,
         ap.REGION_MERCHANT_NUM,
         ap.PHOTO_REQUIRED_IND,
         ap.FROM_PARTNER_NVAL,
         ap.ABS_NUM,
         ap.EXPORT_IND,
         ap.CURRENT_VERSION_CVAL,
         ap.BIZ_VERSION_NVAL,
         ap.ISSUE_TYPE_NAME,
         ap.CREDIT_CARD_OFFERPRT_IND,
         ap.DEBIT_CARD_CONTRACT_NUM,
         ap.UNIVERSAL_CARD_CONTRACT_NUM,
         ap.OPERATOR_NAME,
         ap.CIF_CONTRACT_NUM,
         ap.AUTHORIZE_USER_LOGIN_NAME,
         ap.BCH_REQUEST_IND,
         ap.BUREAU_PMT_AMT,
         ap.INTERNAL_PMT_AMT,
         ap.BP_BALANCE_NVAL,
         ap.INCOME_ADJ_AMT,
         ap.POSITIVE_CRED_HIST_NVAL,
         ap.PRODUCT_PTI_FACT_AMT,
         ap.PTI_FINAL_AMT,
         ap.RISK_CLASS_NVAL,
         ap.SPARE_PTI_NVAL,
         ap.AML_IND,
         ap.BP_BALANCE_SEQ_NVAL,
         ap.BP_CREDIT_COUNT_NVAL,
         ap.RISK_QUEUE_NVAL,
         ap.DAR_PADJ_AMT,
         ap.EARLY_REP_AMT,
         ap.DOC_PACKAGE_NVAL,
         ap.DOC_CLASS_NAME,
         ap.PROD_CODE,
         ap.PARENT_CLONE_CODE,
         ap.PARENT_DOUBLE_CODE,
         ap.CUST_CATEGORY_NVAL,
         ap.COMBI_ISSUE_IND,
         ap.COMBI_OFFER_IND as combi_offer_flag,
         ap.COMBI_LIMIT_AMT,
         ap.SCAN_DOC_TYPE_TXT,
         ap.LOGIN_ISSUED_NAME,
         ap.LOGIN_AUTH_NAME,
         ap.REGION_MERCHANT_ISS_NUM,
         ap.PHOTO_IND,
         ap.ORIG_POS_ID,
         ap.FIXPAYMENT_NVAL,
         ap.FIRST_FORM_TYPE_NVAL,
         ap.FORM_TYPE_NVAL,
         ap.AUTH_TYPE_NVAL,
         ap.DOCSIGN_TYPE_NVAL,
         ap.retention_ind,
         ap.CVPVALID_NVAL,
         ap.GOODSTYPE_MAX_NVAL,
         ap.POS_CHANNEL_CVAL,
         ap.POS_UPPER_CHANNEL_CVAL,
         ap.APPIBTRANSFER_DT,
         ap.ADDONIPDV_IND,
         ap.DWH_SRC_KEY,
         ap.RF_REGION_KEY,
         ap.TOP_UP_GP_IND,
         al.subject_num as client_number, 
         case 
           when ap.dwh_src_key = 3 then 0 
           when ap.dwh_src_key = 7 then 1 
         end as flg_ncc
    from app.DIM_APPLICATION ap
    join app.dim_applicant al
      on ap.application_key = al.application_key
     and ap.dwh_src_key = al.dwh_src_key
   where trunc(system_open_dt) >= date'2020-08-01'
    --to_date('01.07.2019','dd.mm.yyyy')
    and POS_ID not in (136043,46533,57583,53004,49482,56057,106584,
					   59264,103,10298,38166,240934,23039,54803,56038,414293);
  
  commit;
  
  --dbms_stats.gather_table_stats('userbox_risk_mis', 'dev_application_incr');
  
  --userbox_risk_mis.pkg_log.step_end;
end;

/*??????? DEV_PROD_EXCEPTION_SEGMENT ???????????? ??? ?????????? ?? ????????? CONF_PROD_CODE.
? SAS ???? next_date ??????????? ??????? ???????, ???? ???? ????????? ????? ? ????? prod_id.
?? ??? ??? ????? ????? ???, ?? ???? ??????????? ????? ? ??? ?? ?????????.
??????? ? SAS ?? ??????????? ? ?????? 2019 ????.

update userbox_risk_mis.DEV_PROD_EXCEPTION_SEGMENT t
set next_date = to_date('31.12.9999','dd.mm.yyyy');*/

procedure dev_segment_exception
is
begin
  --userbox_risk_mis.pkg_log.step_begin;
  
  execute immediate 'truncate table userbox_risk_mis.dev_application_incr_segment_exp';
  
   insert /*+ parallel(12) */ into userbox_risk_mis.dev_application_incr_segment_exp
   select /*+ parallel(12) */ 
          APPLICATION_KEY,
          APPLICATION_NUM,
          APPLICATION_CODE,
          APPCHNNLTYPE_ID,
          REGPOS_KEY,
          REGPOS_ID,
          POS_KEY,
          POS_ID,
          USDEXRATE_NVAL,
          EUROEXRATE_NVAL,
          SALESMAN_NAME,
          ISOFFLINE_IND,
          POSAUTH_CVAL,
          REPORT_DELIVERY_ADDRESS_NAME,
          BRANCH_NUM,
          SYSTEM_OPEN_DT,
          CQUEUESTATE_NAME,
          STATUS_DT,
          STATUS_17_IND,
          CONTRACT_KEY,
          CONTRACT_NUM,
          CONTRACT_SOURCE_SYSTEM_KEY,
          ABSCONTRACT_NUM,
          CREDITCARD_NUM,
          PRODUCT_KEY,
          CONF_PROD_CODE,
          REQ_PROD_CODE,
          PURCHASEAMOUNT_MAX_NVAL,
          PDSGOODSGROUP_MAX_NAME,
          PDSGOODSCAT_MAX_NAME,
          PURCHASEAMOUNT_SUM_NVAL,
          PDSNBRUNITS_SUM_CNT,
          INSURLIFE_AMT,
          INSURUNEMPLOYM_AMT,
          REGION_MERCHANT_NAME,
          REGION_KEY,
          RISK_APPROVE_IND,
          CAPP_PRODUCTGROUP_NAME,
          PORTFOLIO_KEY,
          PORTFOLIO_NUM,
          case 
            when es.segment_id is not null  
            then es.segment_id 
            else SEGMENT_NUM 
          end as segment_num,
          SUB_SEGMENT_NUM,
          CHANNEL_KEY,
          SUB_CHANNEL_KEY,
          ONLINE_FLAG_IND,
          APPRISKSEGMENT_CVAL,
          APPFRAUDFLAGTYPE_NAME,
          APP_BASE_KEY,
          APP_BASE_CODE,
          SCORE_NVAL,
          RELOAD_STATUS_CVAL,
          DECISION_CVAL,
          PURCHASE_LIST_TXT,
          OFFER_CARD_IND,
          ISS_CARD_IND,
          CONSUMER_REJ_REASONS_NVAL,
          REPRESENTATIVE_CVAL,  
          CONTACT_DENY_IND,
          DELIVERY_TYPE_NAME,
          DELIVERY_MCO_TYPE_NAME,
          CDA_IND,
          AMOUNT_NVAL,
          REQUEST_AMOUNT_NVAL,
          RATE_NVAL,
          REQUESTED_RATE_NVAL,
          REQUEST_TERM_NVAL,
          OFFERED_TERM_NVAL,
          CURRENCY_NAME,
          ASKED_TERM_NVAL,
          REQUEST_START_PAY_NVAL,
          START_PAY_NVAL,
          OPERATOR_LOGIN_NAME,
          PURPOSE_NAME,
          DECISION_NVAL,
          DECISION_DT,
          REASON_CVAL,
          SUBREASON_01_CVAL,
          CREDIT_LIMIT_AMT,
          RATE_PERCENT_NVAL,
          POSID_NAME,
          OFFLINE_IND,
          CHAIN_NAME,
          NETWORK_NAME,
          SMS_INFORM_IND,
          RECEIVE_CHANNEL_NAME,
          MARKETING_CHANNEL_KEY,
          DIVISION_NAME,
          ADDRESS_TXT,
          PHONE_CVAL,
          CREDITLIMIT_AMT,
          CREDIT_CARD_ACTIVATED_IND,
          GP_OFFER_IND,
          CLIENT_DENY_TXT,
          LOGIN_17_NAME,
          APPROVE_IND,
          DECISION_IND,
          REASON_KEY,
          INS_FRAUD_AMT,
          CRED_INS_AMT,
          CRED_INS_TERM_AMT,
          PLT_AMT,
          MERCHANT_INCENTIVE_RATE_NVAL,
          MERCHANT_INCENTIVE_FIX_AMT,
          LIFE_INSURANCE_AMT,
          LIFE_AGREE_IND,
          ILOE_INSURANCE_AMT,
          ILOE_AGREE_IND,
          TO_MERCHANT_AMT,
          AGENT_NUM,
          INSURANCE_COMPANY_NUM,
          DWH_INS_DT,
          DWH_UPD_DT,
          DWH_PROCESS_KEY,
          DWH_CRC_NVAL,
          REG_COUNTRY_CODE,
          CANCEL_REASON_CVAL,
          VERIFICATION_CNT,
          VALID_LOGIN_NAME,
          VALID_DT,
          CLOSE_COMMENT_TXT,
          ITERATION_NUM,
          MFO_IND,
          LOCALS_AGENT_NUM,
          REPORT_DELIVERY_ADDRESS_NUM,
          PURPOSE_NUM,
          APPFRAUDFLAGTYPE_NUM,
          PDSGOODSCAT_MAX_NUM,
          PDSGOODSGROUP_MAX_NUM,
          REGION_MERCHANT_NUM,
          PHOTO_REQUIRED_IND,
          FROM_PARTNER_NVAL,
          ABS_NUM,
          EXPORT_IND,
          CURRENT_VERSION_CVAL,
          BIZ_VERSION_NVAL,
          ISSUE_TYPE_NAME,
          CREDIT_CARD_OFFERPRT_IND,
          DEBIT_CARD_CONTRACT_NUM,
          UNIVERSAL_CARD_CONTRACT_NUM,
          OPERATOR_NAME,
          CIF_CONTRACT_NUM,
          AUTHORIZE_USER_LOGIN_NAME,
          BCH_REQUEST_IND,
          BUREAU_PMT_AMT,
          INTERNAL_PMT_AMT,
          BP_BALANCE_NVAL,
          INCOME_ADJ_AMT,
          POSITIVE_CRED_HIST_NVAL,
          PRODUCT_PTI_FACT_AMT,
          PTI_FINAL_AMT,
          RISK_CLASS_NVAL,
          SPARE_PTI_NVAL,
          AML_IND,
          BP_BALANCE_SEQ_NVAL,
          BP_CREDIT_COUNT_NVAL,
          RISK_QUEUE_NVAL,
          DAR_PADJ_AMT,
          EARLY_REP_AMT,
          DOC_PACKAGE_NVAL,
          DOC_CLASS_NAME,
          PROD_CODE,
          PARENT_CLONE_CODE,
          PARENT_DOUBLE_CODE,
          CUST_CATEGORY_NVAL,
          COMBI_ISSUE_IND,
          COMBI_OFFER_IND as combi_offer_flag,
          COMBI_LIMIT_AMT,
          SCAN_DOC_TYPE_TXT,
          LOGIN_ISSUED_NAME,
          LOGIN_AUTH_NAME,
          REGION_MERCHANT_ISS_NUM,
          PHOTO_IND,
          ORIG_POS_ID,
          FIXPAYMENT_NVAL,
          FIRST_FORM_TYPE_NVAL,
          FORM_TYPE_NVAL,
          AUTH_TYPE_NVAL,
          DOCSIGN_TYPE_NVAL,
          nvl(retention_ind,0) as retention_ind,
          CVPVALID_NVAL,
          GOODSTYPE_MAX_NVAL,
          POS_CHANNEL_CVAL,
          POS_UPPER_CHANNEL_CVAL,
          APPIBTRANSFER_DT,
          ADDONIPDV_IND,
          DWH_SRC_KEY,
          RF_REGION_KEY,
          TOP_UP_GP_IND,
          CLIENT_NUMBER,
          FLG_NCC
     from userbox_risk_mis.dev_application_incr ai
left join DEV_PROD_EXCEPTION_SEGMENT es 
       on ai.conf_prod_code = upper(es.prod_id)
      and ai.system_open_dt >= es.START_DATE 
      and ai.system_open_dt < es.NEXT_DATE
      and ai.portfolio_num = 5;
      
   commit;
   
   
   --userbox_risk_mis.pkg_log.step_end;
end;

procedure dev_final_decision
is
begin
  
  execute immediate 'truncate table userbox_risk_mis.dev_final_decision';
---Добавляем признаки решения по заявке
  insert /*+ parallel(8) */ into userbox_risk_mis.dev_final_decision
  select /*+ parallel(8) */ 
         d.APPLICATION_NUM AS SUPERID,
         d.cdapddecisionresult_num AS CDAPDDECISIONRESULT,
         d.td_ind as td,
         d.cancel_ind as cancel,
         d.finalapprove_ind as finalapprove,
         d.deny_ind as deny
    from app.DIM_FINAL_DECISION d
    join dev_application_incr_segment_exp s
      on d.application_num = s.application_num;
    
  commit;
end;

procedure dev_mfo_flags
is
begin
  
  execute immediate 'truncate table userbox_risk_mis.dev_mfo_flags';
--Добавляем флаги
  insert /*+ parallel(4) */ into userbox_risk_mis.dev_mfo_flags
  select hncid as superid, 
         APPMAXLIMITPROCESS, 
         APPISPAPERESIGN, 
         APPDIFFERENTPHONENMBR, 
         APPMICROCREDITPARTNER,
         -1 as APPMICROCREDITAVAILABLE,
         APPPOSWITHPRODUCTMKK
    from rpl_cdmp_capstone_mirr.applicationuser@ods 
   where hncid >= 2198797385 and 
         (APPMAXLIMITPROCESS is not null or 
         APPISPAPERESIGN is not null or
         APPDIFFERENTPHONENMBR is not null or 
         APPMICROCREDITPARTNER is not null or
         APPPOSWITHPRODUCTMKK is not null);
      
   commit;

end;

procedure dev_declncode_search
is
begin
  execute immediate 'truncate table userbox_risk_mis.dev_declncode_ncc';

  insert /*+ parallel(8) */ into userbox_risk_mis.dev_declncode_ncc
  select /*+ parallel(8) */
         t.application_num as superid, 
         t.cancel_reason_cval,
         t.reason_cval
    from userbox_risk_mis.DEV_APPLICATION_INCR t
   where t.dwh_src_key = 7
     and t.reason_cval like '%240%' 
      or ((replace(t.reason_cval,'Flow','') is null or t.reason_cval not like '%240%') and t.cancel_reason_cval like '%;1%');
     
  commit;  
  
end;

procedure dev_declncode_data
is
  incr number := 4;
  reason_cval varchar2(100) := '';
  order_by number := 1;  
begin
  
  execute immediate 'truncate table userbox_risk_mis.DEV_DECLNCODE_DATA';  
  
  for c in 
  --вытаскиваем все четырехзнаяные коды начинающиеся с единицы
  --и складываем их в порядке следования
    (select z.*, regexp_count(z.cancel_reason_cval, ';1') as res_cnt from userbox_risk_mis.dev_declncode_ncc z)
    loop
      for loop_cnt in 1 .. c.res_cnt--цикл по заявке
        loop
          reason_cval := substr(c.cancel_reason_cval, instr(c.cancel_reason_cval,';1',1,order_by)+1,incr);
          
          insert into userbox_risk_mis.DEV_DECLNCODE_DATA
          values(c.superid, reason_cval, order_by);
          
          order_by := order_by + 1;
        end loop;
        
        order_by := 1;
        commit;
    end loop;
    
    commit;
  
end;

procedure dev_max_order_by
is
begin
  execute immediate 'truncate table userbox_risk_mis.dev_max_order_by';
  
  insert into dev_max_order_by
  select 
     superid, 
     max(order_by_cc) as order_by_cc, 
     max(order_by_gp) as order_by_gp 
   from(
       select 
          superid, 
          max(order_by) as order_by_cc, 
          null as order_by_gp
     from userbox_risk_mis.DEV_DECLNCODE_DATA t 
     join PROD_DICT_DECLINE_CODES_NCC cc
       on t.declncode = cc.code
    where cc.cc = 1
    group by superid
    union all 
      select
          superid, 
          null as order_by_cc,
          max(order_by) as order_by_gp
     from userbox_risk_mis.DEV_DECLNCODE_DATA t 
     join PROD_DICT_DECLINE_CODES_NCC gp
       on t.declncode = gp.code
    where gp.gp = 1
    group by superid
  ) group by superid;
  
  commit;

end;

procedure dev_declncode_result
is
begin
   execute immediate 'truncate table userbox_risk_mis.dev_app_declncode_cc_gp';

   insert /*+ parallel(8) */ into userbox_risk_mis.dev_app_declncode_cc_gp
   select /*+ parallel(8) */
          mob.superid, 
          case 
            when reason_cval not like '%240%' and dd_gp.declncode is null then reason_cval 
            when replace(reason_cval,'Flow','') is null and dd_gp.declncode is not null then dd_gp.declncode
            when reason_cval like '%240%' and dd_gp.declncode is null then '240'
            when reason_cval like '%240%' and dd_gp.declncode is not null then dd_gp.declncode
            else ''
          end as DECDECLINECODE_GP, 
          case 
            when reason_cval not like '%240%' and dd_cc.declncode is null then reason_cval
            when replace(reason_cval,'Flow','') is null and dd_cc.declncode is not null then dd_cc.declncode
            when reason_cval like '%240%' and dd_cc.declncode is null then '240'
            when reason_cval like '%240%' and dd_cc.declncode is not null then dd_cc.declncode
            else ''
          end as DECDECLINECODE_CC
     from dev_declncode_ncc dn
     join dev_max_order_by mob
       on dn.superid = mob.superid
left join DEV_DECLNCODE_DATA dd_gp
       on mob.superid = dd_gp.superid
      and mob.order_by_gp = dd_gp.order_by
left join DEV_DECLNCODE_DATA dd_cc
       on mob.superid = dd_cc.superid
      and mob.order_by_cc = dd_cc.order_by;
      
  commit;    
   
end;

procedure dev_merchant_binding
is
begin
  --userbox_risk_mis.pkg_log.step_begin;
  
  execute immediate 'truncate table userbox_risk_mis.dev_app_MERCHANT_BINDING';

  insert /*+ parallel(12) */ into userbox_risk_mis.dev_app_MERCHANT_BINDING
  select /*+ parallel(12) */
        merchant_key,
        id,
        abs_id,
        segment_2level_id,
        start_dt,
        end_dt,
        case when rnum = 1 then to_date('01.01.1900','dd.mm.yyyy') else eff_dt end as eff_dt,
        exp_dt
    from(
      select /*+ parallel(12) */
            merchant_key,
            id,
            abs_id,
            segment_2level_id,
            start_dt,
            end_dt,
            eff_dt,
            row_number() over (partition by id, abs_id order by EXP_DT) as rnum,
            case 
              when EXP_DT = to_date('16.09.2018','dd.mm.yyyy') 
               and ID not in (218468, 340262, 365723) 
              then to_date('17.09.2018','dd.mm.yyyy') 
              else exp_dt 
            end as exp_dt
       from dimdm.V_DIM_MERCHANT_BINDING
      where not (EFF_DT = to_date('17.09.2018 00:00:00','dd.mm.yyyy hh24:mi:ss') 
        and EXP_DT = to_date('17.09.2018 23:59:59','dd.mm.yyyy hh24:mi:ss'))
    );
    
  commit;   

  --userbox_risk_mis.pkg_log.step_end;
end;

procedure dev_merchant_segments
is
begin
   --userbox_risk_mis.pkg_log.step_begin;
   
   execute immediate 'truncate table userbox_risk_mis.dev_app_segments';

   insert /*+ parallel(12) */ into userbox_risk_mis.dev_app_segments
   select /*+ parallel(12) */   
          a.ID as APPPOSID
         ,a.ABS_ID
         ,b.SEGMENT_2LEVEL_NAME as CHANNEL
         ,c.SEGMENT_1LEVEL_NAME as UPPER_CHANNEL
         ,a.start_dt
         ,a.end_dt
         ,a.EFF_DT
         ,a.EXP_DT
     from dev_app_MERCHANT_BINDING a
left join dimdm.V_DIM_MERCHANT_SEGMENT2LEV b
       on a.SEGMENT_2LEVEL_ID = b.SEGMENT_2LEVEL_ID
      and b.EXP_DT = date'9999-12-31'
left join dimdm.V_DIM_MERCHANT_SEGMENT1LEV c
       on b.SEGMENT_1LEVEL_ID = c.SEGMENT_1LEVEL_ID
      and c.EXP_DT = date'9999-12-31';
      
    commit;

   --userbox_risk_mis.pkg_log.step_end;
end;

procedure dev_score_application
is
begin
   --userbox_risk_mis.pkg_log.step_begin;
   
   execute immediate 'truncate table userbox_risk_mis.dev_score_application';

   insert /*+ parallel(12) */ into userbox_risk_mis.dev_score_application
   select /*+ parallel(12) */ 
          t.score_key,
          t.application_key,
          t.dwh_src_key,
          se.application_num as superid,
          t.score_dt,
          t.app_score_nval as cdascore,
          se.cqueuestate_name as status,
          t.app_score_flags_cval as cdascrname
     from dwh_ref.TFCT_SCORE_APPLICATION t 
     join dev_application_incr_segment_exp se
       on t.application_key = se.application_key
      and t.dwh_src_key = se.dwh_src_key
    where exp_dt = date'9999-12-31';
    
    commit;
    
    --userbox_risk_mis.pkg_log.step_end;
end;

procedure dev_max_score_app
is
begin
  
   execute immediate 'truncate table userbox_risk_mis.dev_max_score_app';

   insert /*+ parallel(8) */ into userbox_risk_mis.dev_max_score_app
   select /*+ parallel(8) */ 
          superid,
          max(score_key) as score_key_max,
          --max(score_key_cdadate) as score_key_cdadate,
          max(score_dt) as cdadate_max,
          max(cdadate_cdascore) as cdadate_cdascore
     from(
       select /*+ parallel(8) */ 
              score_key,
              --max(application_key) as application_key,
              superid,
              score_dt,
              case when cdascore is not null then score_dt end as cdadate_cdascore
              --case when cdascore is not null then score_key end as score_key_cdadate
         from userbox_risk_mis.dev_score_application
     )
     group by superid;
    
  commit;
    
end;

procedure dev_score_app
is
begin
  
   execute immediate 'truncate table userbox_risk_mis.dev_score_app';

   insert /*+ parallel(8) */ into userbox_risk_mis.dev_score_app
   select /*+ parallel(8) */ 
          max(score_key) as score_key,
          max(dwh_src_key) as dwh_src_key,
          superid,
          max(score_key_max) as score_key_max,
          max(score_key_cdadate) as score_key_cdadate,
          case when max(score_key_max) = max(score_key_cdadate) and max(score_key_cda) is null then max(score_key_cdadate) else max(score_key_cda) end as score_key_cda,
          max(application_key) as application_key,
          max(status) as status,
          max(cdascrname) cdascrname
      from(
       select /*+ parallel(8) */ 
              s.score_key,
              s.dwh_src_key,
              s.superid,
              m.score_key_max,
              case when m.cdadate_max = s.score_dt then s.score_key else null end as score_key_cdadate,
              case when m.cdadate_cdascore = s.score_dt then s.score_key else null end as score_key_cda,
              s.application_key,
              s.status,
              s.cdascrname
         from userbox_risk_mis.dev_score_application s
         join userbox_risk_mis.dev_max_score_app m
           on s.superid = m.superid
    )group by superid;
    
  commit;
    
end;

procedure dev_app_attr
is
begin
   
   execute immediate 'truncate table userbox_risk_mis.dev_app_attr';

   insert /*+ parallel(8) */ into userbox_risk_mis.dev_app_attr
   select /*+ parallel(8) */ 
          product_class_cval as product_class,
          application_key,
          dwh_src_key,
          a.dbo_contract_num,
          a.asked_mfo_ind as CCDORIGINATORASKED
     from dwh_ref.dim_application_attr a
    where exp_dt = date'9999-12-31'
      and dwh_src_key = 7;
    
  commit;

end;

procedure dev_app_score_attr
is
begin
   
  execute immediate 'truncate table userbox_risk_mis.dev_app_score_attr';

   insert /*+ parallel(8) */ into userbox_risk_mis.dev_app_score_attr
   select /*+ parallel(8) */ 
          scorecard_key,
          score_attr_name,
          score_attr_nval
     from dwh_ref.dim_score_attr
    where exp_dt = date'9999-12-31'
      and score_attr_name in ('Ann_Internal');
    
  commit;

  
end;

procedure dev_score_flag
is
begin
  
   execute immediate 'truncate table userbox_risk_mis.dev_score_flag_app';

   insert /*+ parallel(12) */ into userbox_risk_mis.dev_score_flag_app
   select /*+ parallel(12) */ 
          flag_nval,
          flag_cval,
          score_key,
          score_flag_nval
     from dwh_ref.dim_score_flag 
    where exp_dt = date'9999-12-31'
      and flag_cval in ('RatingAdj','FinalGrade','RatingOrig','TopUpInternet',
                        'TopUpOffer','CCXsellOffer','GPXsellOffer');
  
   commit;
    
end;

procedure dev_application_d_rating
is
begin
  --userbox_risk_mis.pkg_log.step_begin;
  
  execute immediate 'truncate table userbox_risk_mis.DEV_APP_D_RATING';
  
  insert /*+ parallel(12) */ into userbox_risk_mis.DEV_APP_D_RATING
  with rating as (select /*+ parallel(12) */
                           flag_nval,
                           flag_cval,
                           score_key,
                           score_flag_nval,
                           decode(trim(flag_cval),'FinalGrade',1,'RatingAdj',2,'RatingOrig',3) as rating_code
                      from userbox_risk_mis.dev_score_flag_app
                     where flag_cval in ('RatingAdj','FinalGrade','RatingOrig'))
  select /*+ parallel(12) */
         flag_nval,
         superid,
         RatingOrig
   from(                    
      select /*+ parallel(12) */
             z.superid,
             flag_nval,             
             max(case when flag_cval = 'RatingOrig' then flag_nval end) over (partition by z.superid order by score_flag_nval desc) as RatingOrig,
             row_number() over (partition by z.superid order by rating_code asc, score_flag_nval desc) as rnum
        from rating r
        join userbox_risk_mis.dev_score_application z
          on r.score_key = z.score_key
  ) where rnum = 1;
        
    commit;
    
   --userbox_risk_mis.pkg_log.step_end;
end;

procedure dev_app_offer
is
begin
  
  execute immediate 'truncate table userbox_risk_mis.dev_app_offer';
  
  insert /*+ parallel(12) */ into userbox_risk_mis.dev_app_offer
  select /*+ parallel(12) */
          max(case when flag_cval = 'TopUpOffer' then flag_nval end) as TopUpOffer,
          max(case when flag_cval = 'CCXsellOffer' then flag_nval end) as CCXsellOffer,
          max(case when flag_cval = 'GPXsellOffer' then flag_nval end) as GPXsellOffer,
          max(case when flag_cval = 'TopUpInternet' then flag_nval end) as TopUpInternet,
          score_key
      from userbox_risk_mis.dev_score_flag_app
     where flag_cval in ('TopUpOffer','CCXsellOffer','GPXsellOffer','TopUpInternet')
     group by score_key;
        
    commit;
    
end;

procedure dev_app_score_decision
is
begin
  execute immediate 'truncate table userbox_risk_mis.dev_score_decision';
  
  insert /*+ parallel(8) */ into userbox_risk_mis.dev_score_decision
  select /*+ parallel(8) */ 
         score_key,
         max(SCORE_PRODUCT_CATEGORY_CVAL) as PRODUCT_CATEGORY
         /*max(case 
               when reason_txt in ('10028','10005','10009','10010','10011','10012','10055','10422','10318','10001','10002',
                                   '10003','10004','10006','10070','10072','10073','10103','10104','10106') 
               then 1 
         end) as CREDDOC_FLG*/
    from dwh_ref.DIM_SCORE_DECISION 
   where exp_dt = date'9999-12-31'
    group by score_key;
    
    commit;
end;

procedure dev_application_scorecard
is
begin
  
  execute immediate 'truncate table userbox_risk_mis.dev_app_scorecard';
  
  insert /*+ parallel(8) */ into userbox_risk_mis.dev_app_scorecard
  select /*+ parallel(8) */
         score_key, 
         max(scorecard_key) as scorecard_key, 
         max(case when scorecard_name = 'approved_cc' then to_number(substr(score_txt,1,1)) end) as Approved_cc,
         max(case when scorecard_name = 'approved_gp' then to_number(substr(score_txt,1,1)) end) as Approved_gp
    from dwh_ref.dim_scorecard
   where scorecard_name in ('approved_cc', 'approved_gp','PTI')
     and exp_dt = date'9999-12-31'
     --and dwh_src_key = 7
   group by score_key;
   
   commit;
end;

procedure dev_application_pti
is
begin
  
  execute immediate 'truncate table userbox_risk_mis.dev_app_pti';
  
  insert /*+ parallel(8) */ into userbox_risk_mis.dev_app_pti
  select /*+ parallel(8) */
         t.score_key,
         aa.score_attr_nval as Ann_internal
    from userbox_risk_mis.DEV_APP_SCORECARD t 
    join dev_app_score_attr aa
      on t.scorecard_key = aa.scorecard_key
   where aa.score_attr_name is not null;
   
   commit;
end;

procedure dev_app_score_flags
is
begin
  
  execute immediate 'truncate table userbox_risk_mis.dev_app_flags';
  
   insert /*+ parallel(8) */ into userbox_risk_mis.dev_app_flags
   select /*+ parallel(8) */
          superid, 
          max(product_class) as product_class, 
          max(TopUpOffer) as TopUpOffer, 
          max(CCXsellOffer) as CCXsellOffer, 
          max(GPXsellOffer) as GPXsellOffer, 
          max(product_category) as product_category,
          max(approved_cc) as approved_cc, 
          max(approved_gp) as approved_gp,
          max(Ann_internal) as Ann_internal,
          max(TopUpInternet) as TopUpInternet,
          max(rating) as Rating,
          max(dbo_contract_num) as dbo_contract_num,
          max(ccdoriginatorasked) as ccdoriginatorasked,
          max(appmaxlimitprocess) as appmaxlimitprocess,
          max(appispaperesign) as appispaperesign,
          max(appdifferentphonenmbr) as appdifferentphonenmbr,
          max(IsMKKPartner) as IsMKKPartner,
          max(cdascrname) as cdascrname,
          max(ratingorig) as ratingorig,
          max(IsMKKPOS) as IsMKKPOS,
          max(IsМККAccept) as IsМККAccept
     from(
           select /*+ parallel(8) */
                  t.superid, 
                  aa.product_class, 
                  TopUpOffer,
                  CCXsellOffer,
                  GPXsellOffer, 
                  TopUpInternet,
                  r.rating,
                  cf.product_category,  
                  case when substr(t.status,1,3) = '18.' then 0 else sc.approved_cc end as approved_cc, 
                  case when substr(t.status,1,3) = '18.' then 0 else sc.approved_gp end as approved_gp,
                  pt.Ann_internal,
                  aa.dbo_contract_num,
                  aa.ccdoriginatorasked,
                  mf.appmaxlimitprocess,
                  mf.appispaperesign,
                  mf.appdifferentphonenmbr,
                  mf.appmicrocreditpartner as IsMKKPartner,
                  mf.appposwithproductmkk as IsMKKPOS,
                  case when appmicrocreditpartner = 1 and appposwithproductmkk = 1 then 1 else 0 end as IsМККAccept,
                  decode(t.cdascrname,'0','') as cdascrname,
                  r.ratingorig        
             from userbox_risk_mis.DEV_SCORE_APP t
        left join userbox_risk_mis.dev_app_attr aa 
               on t.application_key = aa.application_key
              and t.dwh_src_key = aa.dwh_src_key
        left join userbox_risk_mis.DEV_APP_D_RATING r
               on t.superid = r.superid
        left join userbox_risk_mis.dev_mfo_flags mf
               on t.superid = mf.superid
        left join userbox_risk_mis.dev_score_decision cf
               on t.score_key = cf.score_key
        left join userbox_risk_mis.dev_app_scorecard sc
               on t.score_key_cdadate = sc.score_key
        left join userbox_risk_mis.dev_app_pti pt
               on t.score_key_cda = pt.score_key
        left join userbox_risk_mis.dev_app_offer o
               on t.score_key_cdadate = o.score_key
   )
   group by superid;
       
  commit;

end;

procedure dev_app_data
is
begin
         execute immediate 'truncate table userbox_risk_mis.dev_app_data';
        
         insert /*+ parallel(8) */ into userbox_risk_mis.dev_app_data
         select /*+ parallel(8) */
                t.dwh_src_key,
                t.application_num as superid, 
                case  
                  when flg_ncc = 1 and af.product_class in ('GP','CC') then 9 --для заявок НКК 
                  when (t.segment_num in (51, 52, 53) or 
                       (t.system_open_dt >= to_date('01.01.2017','dd.mm.yyyy') 
                       and CONF_PROD_CODE in ('RCCFIL_DOST299','RCCF13_DOST299','RCCFIL_COMF26',
                                              'RCCF13_COMF26','RCCFIL_COMF28','RCCF13_COMF28',
                                              'RCCFRUIL_1PM20','RCCF13_1PM20','RCCFRUIL_12PM0',
                                              'RCCF13_12PM0','RCCFRUIL_12PM10','RCCF13_12PM10',
                                              'RCCFRUIL_DS29','RCCF13_DS29','RCCF01_CL','RCCF13_CL'))
                        or CONF_PROD_CODE in ('RCCF13_BP1324', 'RCCF13_DOST1324', 'RCCF13_UNIV1324', 
                                              'RCCFRUIL_BP1324', 'RCCFRUIL_DS1324','RCCFRUIL_UN1324', 
                                              'RCCF13_BP612', 'RCCFRUIL_BP612'))
                       and af.product_class not in ('GP','CC') then 5 
                  when (t.pos_id in (288329,320578,322691) and af.product_class not in ('GP','CC') 
                        and t.segment_num = 52) or t.CAPP_PRODUCTGROUP_NAME = '48'  then 4
                  else t.portfolio_num 
                end as portfolio_id, 
                t.system_open_dt as RCLDATERCVDSYS,                 
                case 
                  when (t.system_open_dt >= to_date('01.01.2017','dd.mm.yyyy') 
                   and CONF_PROD_CODE in ('RCCFIL_DOST299','RCCF13_DOST299','RCCFIL_COMF26',
                                          'RCCF13_COMF26','RCCFIL_COMF28','RCCF13_COMF28',
                                          'RCCFRUIL_1PM20','RCCF13_1PM20','RCCFRUIL_12PM0',
                                          'RCCF13_12PM0','RCCFRUIL_12PM10','RCCF13_12PM10',
                                          'RCCFRUIL_DS29','RCCF13_DS29','RCCF01_CL','RCCF13_CL'))
                    or CONF_PROD_CODE in ('RCCF13_BP1324', 'RCCF13_DOST1324', 'RCCF13_UNIV1324', 
                                          'RCCFRUIL_BP1324', 'RCCFRUIL_DS1324','RCCFRUIL_UN1324', 
                                          'RCCF13_BP612', 'RCCFRUIL_BP612')
                   and af.product_class not in ('GP','CC')
                  then 53
                  when (t.pos_id in (288329,320578,322691) or t.FROM_PARTNER_NVAL = 21) and t.segment_num in (41,52) 
                  then 46
                  when t.segment_num = 41 and t.FROM_PARTNER_NVAL = 3
                  then 45
                  when t.CAPP_PRODUCTGROUP_NAME = '48' 
                  then 44
                  when flg_ncc = 1 then --сегмент для заявок НКК
                    case 
                      when af.product_class in ('GP','CC') and (af.topupoffer = 1 or af.ccxselloffer = 1 or af.gpxselloffer = 1) 
                      then 91 --XS
                      when af.product_class in ('GP','CC') and (af.topupoffer = 0 and af.ccxselloffer = 0 and af.gpxselloffer = 0)
                      then 92 --NTB
                    end
                  else t.segment_num
                end as segment_id,
                t.pos_id as appposid,
                t.FROM_PARTNER_NVAL as FROM_PARTNER,
                t.CONF_PROD_CODE as CONF_PROD_ID,
                t.CAPP_PRODUCTGROUP_NAME as CAPP_PRODUCTGROUP,
                t.combi_offer_flag,
                af.rating, 
                t.retention_ind,
                mid.sub_channel_il,
                mid.sub_channel_gp_ntb,
                mid.sub_channel_gp_xs,
                mid.sub_channel_cc_xs,
                mid.sub_channel_cc_ntb,
                case when t.dwh_src_key = 7 then sabs.channel else sid.channel end as channel,
                case when t.dwh_src_key = 7 then sabs.upper_channel else sid.upper_channel end as upper_channel,
                t.isoffline_ind as APPISOFFLINE,
                osc.segment_id as old_segment_id, 
                osc.base_channel,
                app_base_code,
                application_code,
                t.amount_nval,
                t.life_agree_ind as ins_l,
                t.iloe_agree_ind as ins_w,
                abscontract_num,
                rate_nval,
                client_number,
                regpos.channel as channel_out,
                t.regpos_id as appregpos_id,
                sc.sub_channel as sub_channel_ncc,
                flg_ncc,
                af.product_class,
                af.topupoffer,
                af.ccxselloffer,
                af.gpxselloffer,
                af.product_category,
                af.approved_cc,
                af.approved_gp,
                ad.decdeclinecode_gp,
                ad.decdeclinecode_cc,
                case 
                  when (t.CAPP_PRODUCTGROUP_NAME = '36' and af.topupinternet = 1) or 
                        t.CAPP_PRODUCTGROUP_NAME = '34'
                  then af.ann_internal
                end as BASEPMT,
                fd.cdapddecisionresult,
                t.decision_nval,
                t.cqueuestate_name,
                t.reason_cval,
                nvl(t.mfo_ind,1) as ccdoriginatorconfirmed,
                af.ccdoriginatorasked,
                case when af.dbo_contract_num is not null and nvl(appdifferentphonenmbr, 0) = 0 then 1 else 0 end as IsRep,
                af.appmaxlimitprocess,
                af.appispaperesign,
                af.IsMKKPartner,
                fd.td,
                fd.cancel,
                fd.finalapprove,
                fd.deny,
                af.cdascrname,
                af.ratingorig,
                t.cancel_reason_cval as cda_decline_reasons,
                case 
                  when t.cancel_reason_cval like '%;9802;%' and trunc(t.system_open_dt) <= date'2020-03-11' then 1
                  when t.cancel_reason_cval like '%;9602;%' and trunc(t.system_open_dt) >= date'2020-05-27' then 1
                  else 0
                end as HITEQUIFAX,
                case 
                  when t.cancel_reason_cval like '%;9817;%' and trunc(t.system_open_dt) <= date'2020-03-11' then 1
                  when t.cancel_reason_cval like '%;9617;%' and trunc(t.system_open_dt) >= date'2020-05-27' then 1
                  else 0
                end as HITRS,
                case 
                  when t.cancel_reason_cval like '%;9803;%' and trunc(t.system_open_dt) <= date'2020-03-11' then 1
                  when t.cancel_reason_cval like '%;9603;%' and trunc(t.system_open_dt) >= date'2020-05-27' then 1
                  else 0
                end as HITEXPERIAN,
                case 
                  when t.cancel_reason_cval like '%;9801;%' and trunc(t.system_open_dt) <= date'2020-03-11' then 1
                  when t.cancel_reason_cval like '%;9601;%' and trunc(t.system_open_dt) >= date'2020-05-27' then 1
                  else 0
                end as HITNBKI,
                case 
                  when t.cancel_reason_cval like '%;9486;%' then 1
                  else 0
                end as HITANY_SF,
                af.isмккaccept,
                case 
                  when t.cancel_reason_cval like '%;9830;%' then '830'
                  when t.cancel_reason_cval like '%;9241;%' then '240'
                  else reason_cval
                end as DECDCLNCODE_IL_BANK,
                case 
                  when af.isмккaccept = 1 then
                    case 
                      when t.cancel_reason_cval like '%;9242;%' then '240'
                      else reason_cval
                    end
                end as DECDCLNCODE_IL_МКК
           from userbox_risk_mis.dev_application_incr_segment_exp t
      left join userbox_risk_mis.dev_merchant mid
             on t.pos_id = mid.id
      left join userbox_risk_mis.dev_old_sub_channel osc
             on t.app_base_code = osc.base_code
      left join userbox_risk_mis.dev_app_segments sid
             on t.pos_id = sid.appposid
            and trunc(t.system_open_dt) >= sid.start_dt 
            and trunc(t.system_open_dt) <= sid.end_dt
            and trunc(sid.exp_dt) = date'9999-12-31'
      left join userbox_risk_mis.dev_app_segments sabs
             on t.pos_id = sabs.abs_id
            and trunc(t.system_open_dt) >= sabs.start_dt 
            and trunc(t.system_open_dt) <= sabs.end_dt
            and trunc(sabs.exp_dt) = date'9999-12-31'
      left join userbox_risk_mis.dev_app_segments regpos
             on t.regpos_id = regpos.appposid
            and trunc(t.system_open_dt) >= regpos.start_dt 
            and trunc(t.system_open_dt) <= regpos.end_dt
            and trunc(regpos.exp_dt) = date'9999-12-31'
      left join userbox_risk_mis.dev_sub_channel sc --sub_channel для заявок НКК считается по новому справочнику
             on sc.abs_id = t.pos_id
      left join userbox_risk_mis.dev_app_flags af
             on t.application_num = af.superid
      left join userbox_risk_mis.dev_app_declncode_cc_gp ad
             on t.application_num = ad.superid
      left join userbox_risk_mis.dev_final_decision fd
             on t.application_num = fd.superid;
            
commit;

end;

/*procedure dev_app_state_flags
is
begin
  
  execute immediate 'truncate table userbox_risk_mis.dev_app_state_flags';
  
  insert \*+ parallel(8) *\ into userbox_risk_mis.dev_app_state_flags
  select \*+ parallel(8) *\
         superid,
         TD,
         FINALAPPROVE,
         DENY,
         case 
           when flg_ncc = 0 then
             case 
               when (SEGMENT_ID in (31,33,41,42,45,46,71) or CAPP_PRODUCTGROUP = '43') and 
                  substr(cqueuestate_name,1,3) = '19.' and DENY = 0 and TD = 1
		           then 1
	             when (PORTFOLIO_ID = 5 or CAPP_PRODUCTGROUP = '48') and TD = 1 and DENY = 0 and
                    ((last_mco_reason between '850' and '869') or
                     (last_mco_reason between '870' and '872') or 
                      instr(last_mco_reason,'960') > 0)
		           then 1
	         \*MIS - 455 флаг cancel для 13 ПГ*\
	             when CAPP_PRODUCTGROUP = '13' and DENY = 0 and substr(cqueuestate_name,1,3) = '19.'
		           then 1
		           else 0
             end
           when flg_ncc = 1 then
             case 
               when cqueuestate_name = '19. Заявка завершена' and DENY = 0 and TD = 1 
               then 1
               else 0
             end
          end as CANCEL
   from(
      select \*+ parallel(8) *\
             superid,
             portfolio_id,
             segment_id,
             CAPP_PRODUCTGROUP,
             TD,
             case when flg_ncc = 0 then FINALAPPROVE
               when flg_ncc = 1 then
                 case when substr(cqueuestate_name,1,3) = '17.' or
                     (DENY != 1 and TD = 1) 
                  then 1
                  else 0
                 end
              end as FINALAPPROVE,
             case when flg_ncc = 1 then DENY
               when flg_ncc = 0 then 
                 case when \*Deny GP: + XSELL: + REFINANCE*\
                    SEGMENT_ID in (31,42,32,33,71) and (CDAPDDECISIONRESULT = 0 or decision_nval = 3) and TD = 1
                 then 1
                 else DENY
                 end
             end as DENY,
             decision_nval,
             CDAPDDECISIONRESULT,
             cqueuestate_name,
             last_mco_reason,
             flg_ncc,
             reason_cval
        from(
            select \*+ parallel(8) *\
                   superid,
                   portfolio_id,
                   segment_id,
                   CAPP_PRODUCTGROUP,
                   case 
                     when flg_ncc = 0 then 
                       case 
                         when SEGMENT_ID in (31,32,33,42) and
                           ( (CDAPDDECISIONRESULT is null and instr(cqueuestate_name,'19.') > 0 and decision_nval != 3) or  
                             (trunc(RCLDATERCVDSYS) < date'2011-04-25' and trim(substr(cqueuestate_name,1,3)) in ('10.','11.','')) or
                             (trunc(RCLDATERCVDSYS) >= date'2011-04-25' and trim(substr(cqueuestate_name,1,3)) in ('08.','8.','')))
                         then 0
                         \*IL и CC*\
                         when ((PORTFOLIO_ID in (4,5) or CAPP_PRODUCTGROUP = '43') and SEGMENT_ID != 42 and
                                FINALAPPROVE != 1 and DENY != 1)
                         then 0
                         \*REFINANCE*\
                         when SEGMENT_ID = 71 and ((CDAPDDECISIONRESULT is null and instr(cqueuestate_name,'19.') > 0 and decision_nval != 3) or 
                              trim(substr(cqueuestate_name,1,3)) in ('10.','11.',''))
                         then 0
                         else 1
                       end
                     when flg_ncc = 1 then TD
                   end as TD,
                   case 
                     when flg_ncc = 1 then
                       case 
                         when substr(cqueuestate_name,1,3) = '17.' then 0
                         when (CDAPDDECISIONRESULT = 0 or decision_nval = 3) 
                              and TD = 1 and substr(cqueuestate_name,1,3) <> '17.'
                         then 1
                         else 0
                       end
                      when flg_ncc = 0 then DENY
                   end as DENY,         
                   FINALAPPROVE,
                   decision_nval,
                   CDAPDDECISIONRESULT,
                   cqueuestate_name,
                   last_mco_reason,
                   flg_ncc,
                   reason_cval
              from(
                select \*+ parallel(8) *\
                       superid,
                       portfolio_id,
                       RCLDATERCVDSYS,
                       segment_id,
                       CAPP_PRODUCTGROUP,
                       case when flg_ncc = 0 then--DENY СКК
                        case 
                          when (PORTFOLIO_ID in (4,5) or CAPP_PRODUCTGROUP = '43') and SEGMENT_ID != 42 and 
                              (((cdapddecisionresult = 0 or decision_nval = 3) and 
                                substr(cqueuestate_name,1,3) not in ('17.','14.','15.','12.') ) or
                               (cdapddecisionresult != 0 and substr(cqueuestate_name,1,3) not in ('17.') and
                               (instr(cancel_reason_cval,'10810') > 0 or instr(cancel_reason_cval,'10811') > 0 or
                                instr(cancel_reason_cval,'10875') > 0 or instr(cancel_reason_cval,'10876') > 0 or
                                instr(cancel_reason_cval,'10877') > 0 or instr(cancel_reason_cval,'10881') > 0 or
                                instr(cancel_reason_cval,'10884') > 0 or instr(cancel_reason_cval,'10890') > 0 or
                                instr(cancel_reason_cval,'10891') > 0 or instr(cancel_reason_cval,'10895') > 0 or
                                instr(cancel_reason_cval,'10950') > 0 or instr(cancel_reason_cval,'867') > 0 ) 
                               ) 
                              ) 
                          then 1
                          when (PORTFOLIO_ID = 5 or CAPP_PRODUCTGROUP = '48') and substr(cqueuestate_name,1,3) = '17.' then 0
                          else 0
                        end 
                       end as DENY,
                       case when flg_ncc = 1 then--TD НКК
                         case 
                           when cdapddecisionresult is null and decision_nval = 3 and substr(cqueuestate_name,1,3) = '19.' then 0
                           when cqueuestate_name = '8. Ввод анкеты' or cqueuestate_name is null then 0
                           else 1
                         end
                       end as TD,
                       case when flg_ncc = 0 then--FINALAPPROVE СКК
                         case 
                           when ((PORTFOLIO_ID = 4 and CAPP_PRODUCTGROUP <> '48') or CAPP_PRODUCTGROUP = '43') and 
                                (( CDAPDDECISIONRESULT in (2,3,4) and instr(cancel_reason_cval,'10810') = 0 and 
                                   instr(cancel_reason_cval,'10811') = 0 and instr(cancel_reason_cval,'10875') = 0 and 
                                   instr(cancel_reason_cval,'10876') = 0 and instr(cancel_reason_cval,'10877') = 0 and
                                   instr(cancel_reason_cval,'10881') = 0 and instr(cancel_reason_cval,'10884') = 0 and 
                                   instr(cancel_reason_cval,'10890') = 0 and instr(cancel_reason_cval,'10891') = 0 and 
                                   instr(cancel_reason_cval,'10895') = 0 and instr(cancel_reason_cval,'10950') = 0 and
                                   instr(cancel_reason_cval,'867') = 0 and decision_nval != 3 ) or
                                  ( (CDAPDDECISIONRESULT  = 5 or CDAPDDECISIONRESULT is null) and decision_nval = 1 ) or
                                  substr(cqueuestate_name,1,3) in ('17.','14.','15.','12.') )
                           then 1
                           when (PORTFOLIO_ID = 5 or CAPP_PRODUCTGROUP = '48') and 
                                (( CDAPDDECISIONRESULT = 4 and instr(cancel_reason_cval,'10810') = 0 and 
                                  instr(cancel_reason_cval,'10811') = 0 and instr(cancel_reason_cval,'10875') = 0 and 
                                  instr(cancel_reason_cval,'10876') = 0 and instr(cancel_reason_cval,'10877') = 0 and 
                                  instr(cancel_reason_cval,'10881') = 0 and instr(cancel_reason_cval,'10884') = 0 and 
                                  instr(cancel_reason_cval,'10890') = 0 and instr(cancel_reason_cval,'10891') = 0 and 
                                  instr(cancel_reason_cval,'10895') = 0 and instr(cancel_reason_cval,'10950') = 0 and 
                                  instr(cancel_reason_cval,'867') = 0 ) or 
                                  ( CDAPDDECISIONRESULT is null and decision_nval = 1 ) or
                                substr(cqueuestate_name,1,3) in ('17.','14.','15.','12.') )
                           then 1
                           else 0
                         end
                       end as FINALAPPROVE,
                       decision_nval,
                       CDAPDDECISIONRESULT,
                       cqueuestate_name,
                       reason_cval,
                       case 
                         when instr(reason_cval,',',-1) > 0 
                         then substr(reason_cval,instr(reason_cval,',',-1)+1)
                         else reason_cval 
                       end as last_mco_reason,
                       flg_ncc
                  from userbox_risk_mis.dev_app_data
               )
           )
  );

  commit;

end;*/
  
procedure dev_application_subchannel
is
begin
  --userbox_risk_mis.pkg_log.step_begin;
  
   execute immediate 'truncate table userbox_risk_mis.dev_old_sub_channel';
  
   insert /*+ parallel(12) */ into userbox_risk_mis.dev_old_sub_channel
   select base_code, 
          sub_channel,
          case 
            when (SEGMENT_ID = 31 and BASE_CHANNEL not in ('GP XS_BRANCH','GP XS_CALL_CENTER','GP XS_TM')) 
              or (SEGMENT_ID = 33 and BASE_CHANNEL not in ('GP NTB_BRANCH','GP NTB_WEB','GP NTB_TM'))
 	          then 'OTHER'
            else base_channel
          end as base_channel,
          segment_id
    from(
       select distinct
              ch.app_base_code as base_code, 
              pr.sub_channel,
              case 
                when pr.SEGMENT_ID = 31 then 'GP XS_' || trim(pr.SUB_CHANNEL)
                when pr.SEGMENT_ID = 33 then 'GP NTB_' || trim(pr.SUB_CHANNEL)
              end as base_channel,           
              pr.segment_id 
         from userbox_risk_mis.dev_sas_application ch
         join userbox_risk_mis.dev_sas_application pr
           on ch.app_base_code = pr.application_code
        where trunc(ch.RCLDATERCVDSYS) >= date'2019-05-30'
          and pr.segment_id in (31,33)
    );
    
   commit;
   
   --userbox_risk_mis.pkg_log.step_end;
end;

procedure dev_sas_application
is
  tbl_size number;
begin  
  --userbox_risk_mis.pkg_log.step_begin;
  
  --??? ????????? ????, ?? ??????? ????????? ??????, ????? ??????????? dev_sas_prev_application ?? ?????????? ??????
  --??? ????? ???? ????????? ???? ???
  /*execute immediate 'truncate table userbox_risk_mis.dev_sas_prev_application';
  
  insert \*+ parallel(12) *\ into userbox_risk_mis.dev_sas_prev_application
  select \*+ parallel(12) *\* 
    from dev_sas_application
   where trunc(RCLDATERCVDSYS) < date'2020-08-01'
      or RCLDATERCVDSYS is null;
  
  commit;  */
  
  --execute immediate 'truncate table userbox_risk_mis.dev_sas_application';
  
  /*insert \*+ parallel(12) *\ into userbox_risk_mis.dev_sas_application        
  select \*+ parallel(12) *\*
    from userbox_risk_mis.dev_sas_prev_application;
         
    
  commit;*/
  
  --userbox_risk_mis.pkg_log.step_begin;
  
  delete /*+ parallel(8) */ from userbox_risk_mis.dev_sas_application 
  where trunc(RCLDATERCVDSYS) >= date'2020-08-01';

  commit;
  
  --userbox_risk_mis.pkg_log.step_end;
  
  --userbox_risk_mis.pkg_log.step_begin;
 
  insert /*+ append parallel(10) */ into userbox_risk_mis.dev_sas_application        
  select superid,
         case when product_category = 'RUGP' and flg_ncc = 1 and SUB_CHANNEL = 'TM_PILOT' then 3 else portfolio_id end as portfolio_id,
         RCLDATERCVDSYS,
         case when product_category = 'RUGP' and flg_ncc = 1 and SUB_CHANNEL = 'TM_PILOT' then 
           case when TopUpOffer = 1 or GPXsellOffer = 1 then 31 else 33 end
          else segment_id
         end as segment_id,
         appposid,
         FROM_PARTNER,
         CONF_PROD_ID,
         CAPP_PRODUCTGROUP,
         case 
           when COMBI_OFFER_FLAG = 1 then
             case 
               when SEGMENT_ID = 31 then 'GP_XS'
               when SEGMENT_ID = 33 then 'GP_NTB'
               when portfolio_id = 5 and RCLDATERCVDSYS >= to_date('31.12.2018','dd.mm.yyyy') then 'IL'
               when portfolio_id = 5 then SUB_CHANNEL
             end
         end as COMBI_CHANNEL,
         rating,
         SUB_CHANNEL,
         channel,
         upper_channel,
         app_base_code,
         application_code,
         amount_nval,
         ins_l, 
         ins_w,
         abscontract_num,
         rate_nval,
         client_number,
         channel_out,
         appregpos_id,
         flg_ncc,
         product_class,
         topupoffer,
         ccxselloffer,
         gpxselloffer,
         product_category,
         approved_cc,
         approved_gp,
         decdeclinecode_gp,
         decdeclinecode_cc,
         BASEPMT,
         ccdoriginatorconfirmed,
         td,
         finalapprove,
         deny,
         cancel,
         ccdoriginatorasked,
         isRep,
         appmaxlimitprocess,
         appispaperesign,
         ismkkpartner,
         HITANY_SF,
         HITANY,
         cda_decline_reasons,
         cdascrname,
         ratingorig,
         isмккaccept,
         DECDCLNCODE_IL_BANK,
         DECDCLNCODE_IL_МКК,
         APPROVED_IL_BANK,
		     APPROVED_IL_MKK
   from(
      select d.superid,
             portfolio_id,
             RCLDATERCVDSYS,
             segment_id,
             appposid,
             FROM_PARTNER,
             CONF_PROD_ID,
             CAPP_PRODUCTGROUP,
             combi_offer_flag,
             rating,
             case 
               when flg_ncc = 0 then
                 case 
                   --????????? ?? ?????? MIS-1634
                   when (APPPOSID = 434399 AND CAPP_PRODUCTGROUP = '03') OR
                        (APPPOSID = 434578 AND CAPP_PRODUCTGROUP = '34') OR
                        (APPPOSID = 434579 AND CAPP_PRODUCTGROUP = '31') 
                   then 'STAFF'
                   --when creddoc_flg = 1 and trunc(RCLDATERCVDSYS) < to_date('31.10.2018','dd.mm.yyyy') then 'CREDIT DOCTOR'
                   when SEGMENT_ID = 33 and retention_ind = 1 and APPPOSID not in ( 434399, 434578, 434579) then 'RETENTION'
                   when SEGMENT_ID = 33 and retention_ind != 1 and APPPOSID not in ( 434399, 434578, 434579) then SUB_CHANNEL_GP_NTB
                   when SEGMENT_ID = 31 and retention_ind = 1 and APPPOSID not in ( 434399, 434578, 434579) then 'RETENTION'
                   when SEGMENT_ID = 31 and CAPP_PRODUCTGROUP = '36' and retention_ind != 1 and APPPOSID not in (434399, 434578, 434579) then 'INTERNET BANK'
                   when SEGMENT_ID = 31 and CAPP_PRODUCTGROUP != '36' and retention_ind != 1 and APPPOSID not in (434399, 434578, 434579) then SUB_CHANNEL_GP_XS
                   when SEGMENT_ID = 42 and APPPOSID not in (434399, 434578, 434579) then SUB_CHANNEL_CC_XS
                   when portfolio_id = 5 or CAPP_PRODUCTGROUP = '48' then
                     case 
                       when SUB_CHANNEL_IL is not null and SUB_CHANNEL_IL != 'OTHER' then SUB_CHANNEL_IL
                       when (SUB_CHANNEL_IL is null or SUB_CHANNEL_IL = 'OTHER') and APPISOFFLINE = 1 then 'LOCALS_OFFLINE'
                       when (SUB_CHANNEL_IL is null or SUB_CHANNEL_IL = 'OTHER') and (APPISOFFLINE <> 1 or APPISOFFLINE is null) then 'LOCALS_ONLINE' 
                     end
                   when SEGMENT_ID = 41 then
                     case 
                       when APPPOSID in (363224, 363225, 364714) then 'DIKSI'
                       when APPPOSID = 369047 then 'INTERNET BANK'
                       when CONF_PROD_ID in ('RCCFMC_DDMC4240','RCCFMC_DDMA4240','RCCFMC_DMCR4240',
                                             'RCCFMC_DMAR4240','RCCFMC_DMCN4240','RCCFMC_DMAN4240')  then 'VIP'
                       when CONF_PROD_ID in ('RCCFRUDC_DDMC42','RCCFRUDC_DDMA42','RCCFMC_EDMC4240','RCCFMC_EDMA4240',
                                             'RCCFMC_EDMC4249','RCCFMC_EDMA4249','RCCFMG_EDMC4240','RCCFMG_EDMA4240',
                                             'RCCFMC_WSM42_40','RCCFMC_WSA42_40','RCCFMC_WGM42_40','RCCFMC_WGA42_40',
                                             'RM_15_0_41S04_I','RM_15_0_41S04','RM_15_0_41G04_I','RM_15_0_41G04',
                                             '18_18_0_41W04_I','18_18_0_41W04','18_18_0_41_0444','RM_15_0_41W04_I',
                                             'RM_15_0_41W04','RS_15_0_41_0444','RCCFMCS_STF','RCCFRUCC_STF') then 'STAFF CARD'
                       when RCLDATERCVDSYS < to_date('01.08.2019','dd.mm.yyyy') and CAPP_PRODUCTGROUP in ( '41','04') then 'BRANCH'
                       when RCLDATERCVDSYS >= to_date('01.08.2019','dd.mm.yyyy') and CAPP_PRODUCTGROUP in ( '41') then 'BRANCH'
                       when RCLDATERCVDSYS < to_date('01.08.2019','dd.mm.yyyy') and 
                            (CAPP_PRODUCTGROUP = '14' or (APPPOSID in (2209, 213352) and CONF_PROD_ID = '199459_41_04')) then 'WEB'
                       when RCLDATERCVDSYS >= to_date('01.08.2019','dd.mm.yyyy') and CAPP_PRODUCTGROUP in ( '14', '04')  then 'WEB'
                       when CAPP_PRODUCTGROUP = '47' then 'SVYAZNOY'
                       when FROM_PARTNER = 2 then 'EUROSET'
                       when SUB_CHANNEL_CC_NTB != ' ' and SUB_CHANNEL_CC_NTB is not null then SUB_CHANNEL_CC_NTB
                       else 'OTHER'
                     end
                   when SEGMENT_ID = 45 then
                     case 
                       when FROM_PARTNER = 3 then 'BEELINE'
                       when CAPP_PRODUCTGROUP = '47' then 'SVYAZNOY'
                       when CAPP_PRODUCTGROUP = '41' then 'BRANCH'
                       else 'OTHER'
                     end
                   when SEGMENT_ID = 46 then
                     case 
                       when FROM_PARTNER = 21 then 'GOLDEN CROWN'
                       when BASE_CHANNEL != ' ' and BASE_CHANNEL is not null then BASE_CHANNEL
                       else 'OTHER'
                     end
                   when portfolio_id = 7 then
                     case 
                       when CAPP_PRODUCTGROUP = '31' then 'Refinance'
                       when CAPP_PRODUCTGROUP = '34' then 'PaymentRelief'
                     end
                   when portfolio_id = 8 then 'DEBIT'
                 end
               when flg_ncc = 1 then sub_channel_ncc
             end as SUB_CHANNEL,
             channel,
             upper_channel,
             app_base_code,
             application_code,
             amount_nval,
             ins_l,
             ins_w,
             abscontract_num,
             rate_nval,
             client_number,
             channel_out,
             appregpos_id,
             flg_ncc,
             product_class,
             topupoffer,
             ccxselloffer,
             gpxselloffer,
             product_category,
             approved_cc,
             approved_gp,
             decdeclinecode_gp,
             decdeclinecode_cc,
             BASEPMT,
             ccdoriginatorconfirmed,
             d.td,
             d.finalapprove,
             d.deny,
             d.cancel,
             d.ccdoriginatorasked,
             d.isRep,
             d.appmaxlimitprocess,
             d.appispaperesign,
             d.ismkkpartner,
             d.cda_decline_reasons,
             d.cdascrname,
             d.ratingorig,
             d.HITANY_SF,
             case when d.HITNBKI = 1 or d.HITEQUIFAX = 1 or d.HITEXPERIAN = 1 or d.HITRS = 1 then 1 else 0 end as HITANY,
             d.isмккaccept,
             d.DECDCLNCODE_IL_BANK,
             d.DECDCLNCODE_IL_МКК,
             case when d.DECDCLNCODE_IL_BANK is null then 1 else 0 end as APPROVED_IL_BANK,
		         case when d.DECDCLNCODE_IL_МКК is null and d.IsМККAccept = 1 then 1 else 0 end as APPROVED_IL_MKK
        from userbox_risk_mis.dev_app_data d
  );

  commit;
  
  --userbox_risk_mis.pkg_log.step_end;
  
begin
  select sum(t.BYTES/1024/1024/1024) 
    into tbl_size
    from sys.user_SEGMENTS t
   where t.segment_name = 'DEV_SAS_APPLICATION';
     
   if tbl_size > 50 then 
     userbox_risk_mis.p_table_defragmentation('USERBOX_RISK_MIS','DEV_SAS_APPLICATION');
   end if;
end;

  --userbox_risk_mis.pkg_log.step_end;
end;

procedure dev_application_process
is
  cnt_check number;
begin
  PKG_APPLICATION_SAS.dev_application_incr;
  PKG_APPLICATION_SAS.dev_segment_exception;
  PKG_APPLICATION_SAS.dev_final_decision;
  PKG_APPLICATION_SAS.dev_declncode_search;
  PKG_APPLICATION_SAS.dev_declncode_data;
  PKG_APPLICATION_SAS.dev_max_order_by;
  PKG_APPLICATION_SAS.dev_declncode_result;
  PKG_APPLICATION_SAS.dev_merchant_binding;
  PKG_APPLICATION_SAS.dev_merchant_segments;
  PKG_APPLICATION_SAS.dev_score_application;
  PKG_APPLICATION_SAS.dev_max_score_app;
  PKG_APPLICATION_SAS.dev_score_app;
  PKG_APPLICATION_SAS.dev_score_flag;
  PKG_APPLICATION_SAS.dev_app_attr;
  PKG_APPLICATION_SAS.dev_app_score_attr;
  PKG_APPLICATION_SAS.dev_application_d_rating;
  --PKG_APPLICATION_SAS.dev_app_application_limit;
  --PKG_APPLICATION_SAS.dev_app_combi_offer;
  PKG_APPLICATION_SAS.dev_app_score_decision;
  PKG_APPLICATION_SAS.dev_application_scorecard;
  PKG_APPLICATION_SAS.dev_application_pti;
  PKG_APPLICATION_SAS.dev_mfo_flags;
  PKG_APPLICATION_SAS.dev_app_score_flags;
  PKG_APPLICATION_SAS.dev_app_data;
  --PKG_APPLICATION_SAS.dev_app_state_flags;
  PKG_APPLICATION_SAS.dev_application_subchannel;
  PKG_APPLICATION_SAS.dev_sas_application;

  execute immediate 'truncate table userbox_risk_mis.dev_score_application';
  execute immediate 'truncate table userbox_risk_mis.dev_application_incr';
  execute immediate 'truncate table userbox_risk_mis.dev_application_incr_segment_exp';
  execute immediate 'truncate table userbox_risk_mis.dev_final_decision';
  execute immediate 'truncate table userbox_risk_mis.dev_max_score_app';
  execute immediate 'truncate table userbox_risk_mis.dev_app_attr';
  execute immediate 'truncate table userbox_risk_mis.dev_score_flag_app';
  --execute immediate 'truncate table userbox_risk_mis.dev_application_creddoc_flag';
  execute immediate 'truncate table userbox_risk_mis.dev_app_scorecard';
  execute immediate 'truncate table userbox_risk_mis.dev_score_decision';
  execute immediate 'truncate table userbox_risk_mis.dev_app_flags';
  
  --проверяем чтобы не было дублей в витрине
  begin
    select /*+ parallel(8) */nvl(sum(cnt),0) as cnt
      into cnt_check
      from(
        select /*+ parallel(8) */count(*) as cnt
          from dev_sas_application
         group by superid
        having count(*) > 1
      );
      
  if cnt_check = 0 then
    userbox_risk_mis.send_email('idavidovich@rencredit.ru','idavidovich@rencredit.ru,lryzhikova@rencredit.ru,gkhiramanov@rencredit.ru,oshelemetev@rencredit.ru,ilisenkov@rencredit.ru',
                                'Сборка показателей из APPLICATION (SAS) на DWH',
                                'Показатели витрин APPLICATION (SAS) на DWH обновлены');
                                
   -- execute immediate 'truncate table userbox_risk_mis.dev_application_incr';
   -- execute immediate 'truncate table userbox_risk_mis.dev_application_incr_segment_exp';
    
  elsif cnt_check > 0 then
    userbox_risk_mis.send_email('idavidovich@rencredit.ru','idavidovich@rencredit.ru,lryzhikova@rencredit.ru,gkhiramanov@rencredit.ru,oshelemetev@rencredit.ru,ilisenkov@rencredit.ru',
                                'Сборка показателей из APPLICATION (SAS) на DWH',
                                'Показатели витрин APPLICATION (SAS) на DWH обновлены с ошибкой, есть дубли');
  end if;
  
  end;
  
end;

end PKG_APPLICATION_SAS;
/
