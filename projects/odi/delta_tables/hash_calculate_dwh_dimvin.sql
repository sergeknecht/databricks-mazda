-- plsql

-- Example script: calculate checksums PK and DATA - DWH.DIM_VIN
WITH table1 AS (
    SELECT
        *
    FROM
        dwh.dim_vin
)
SELECT
    vin_sk,
    standard_hash(vin_sk, 'MD5')             hash_pk,
    standard_hash(connectivity
                  || create_ts
                  || create_user
                  || etl_job_name
                  || extr_color_sk
                  || intr_color_sk
                  || load_nr
                  || son_nk
                  || source
                  || update_ts
                  || update_user
                  || veh_mdl_loc_sk
                  || veh_mdl_sk
                  || vin_actual_arrival_cmpd_cal_sk
                  || vin_amend_status
                  || vin_bill_of_lading_cal_sk
                  || vin_block_regional
                  || vin_call_off_usr
                  || vin_call_of_cal_sk
                  || vin_cal_dereg_sk
                  || vin_cal_dereg_transact_sk
                  || vin_cal_mobile_activation_sk
                  || vin_cal_mot_next_due_sk
                  || vin_cal_ord_req_dlv_sk
                  || vin_cal_pend_retl_sale_sk
                  || vin_cal_reg_sk
                  || vin_cal_reg_transaction_sk
                  || vin_cal_rsa_end_sk
                  || vin_cal_sara_end_sk
                  || vin_cal_sara_start_sk
                  || vin_car_delivery_address_sk
                  || vin_case_nr
                  || vin_cc_sales_type_code
                  || vin_cc_sub_sales_type_code
                  || vin_coc_dist_sk
                  || vin_coc_lineoff_cal_sk
                  || vin_coc_type_approval_sk
                  || vin_condition_code
                  || vin_corp_sales_code
                  || vin_cost_centre
                  || vin_country_of_origin_sk
                  || vin_country_registration
                  || vin_crm_cust_id
                  || vin_current_cmpd_sk
                  || vin_current_dist_sk
                  || vin_current_dlr_sk
                  || vin_curr_customer_sk
                  || vin_customs_cleared_cal_sk
                  || vin_customs_cleared_flag
                  || vin_cust_contract_cal_sk
                  || vin_cust_tagged_date
                  || vin_cust_tagged_flag
                  || vin_cust_tagged_user
                  || vin_declared_broker_cal_sk
                  || vin_declared_received_cal_sk
                  || vin_delivery_cal_sk
                  || vin_delivery_cmpd_sk
                  || vin_delivery_received_date
                  || vin_dist_deregistration_sk
                  || vin_dist_first_nos_sk
                  || vin_dist_nos_sk
                  || vin_dlr_deregistration_sk
                  || vin_dlr_first_nos_sk
                  || vin_dlr_nos_sk
                  || vin_dlr_region_sk
                  || vin_dlr_tagged_flag
                  || vin_doa_received_cal_sk
                  || vin_doa_sent_cal_sk
                  || vin_doc_city
                  || vin_doc_contact_name
                  || vin_doc_correspondence_type
                  || vin_doc_country
                  || vin_doc_po_box
                  || vin_doc_province
                  || vin_doc_resend_date
                  || vin_doc_return_date
                  || vin_doc_shipping_method
                  || vin_doc_status
                  || vin_doc_status_code
                  || vin_doc_status_description
                  || vin_doc_status_ps_code
                  || vin_doc_status_ps_description
                  || vin_doc_street
                  || vin_doc_street2
                  || vin_doc_street3
                  || vin_doc_street_nr
                  || vin_doc_zip
                  || vin_driver_desc
                  || vin_driver_instruction_1
                  || vin_driver_instruction_2
                  || vin_dsr_cv_mileage_km
                  || vin_dsr_cv_mileage_miles
                  || vin_dsr_cv_mileage_update_date
                  || vin_dsr_last_ar_dist_sk
                  || vin_dsr_last_ar_dlr_sk
                  || vin_dsr_last_cnt_dist_sk
                  || vin_dsr_last_cnt_dlr_sk
                  || vin_dsr_last_svc_dist_sk
                  || vin_dsr_last_svc_dlr_sk
                  || vin_dsr_pdi_dist_sk
                  || vin_dsr_pdi_dlr_sk
                  || vin_dvla_reg_user
                  || vin_earliest_delivery_cal_sk
                  || vin_engine_code
                  || vin_engine_nr
                  || vin_engine_serial_nr
                  || vin_engine_type
                  || vin_est_pdi_date_bundl_cal_sk
                  || vin_est_pdi_date_fdo_cal_sk
                  || vin_est_pdi_ready_cal_sk
                  || vin_eta_nta_date
                  || vin_eta_nta_flag
                  || vin_factory_order_cal_sk
                  || vin_fct_clas
                  || vin_fct_crcy
                  || vin_fdo_cmpd_load_cal_sk
                  || vin_fdo_cmpd_sk
                  || vin_fdo_overrule_cal_sk
                  || vin_fdo_received_cal_sk
                  || vin_fdo_sent_cal_sk
                  || vin_finance_type
                  || vin_first_demo_cal_sk
                  || vin_first_dereg_cal_sk
                  || vin_first_registration_fee
                  || vin_fisc_entity
                  || vin_fr_con_code
                  || vin_fr_sales_sub_type_code
                  || vin_fr_sales_type_code
                  || vin_fr_usage_type
                  || vin_fta
                  || vin_hold_dlr_deregistration_sk
                  || vin_hold_dlr_first_nos_sk
                  || vin_hold_dlr_nos_sk
                  || vin_ibt_incoterm_code
                  || vin_ibt_incoterm_description
                  || vin_ibt_incoterm_name
                  || vin_ibt_price
                  || vin_ibt_price_cur_sk
                  || vin_ibt_price_freight
                  || vin_ibt_price_freight_cur_sk
                  || vin_ibt_price_insurance
                  || vin_ibt_price_insurance_cur_sk
                  || vin_icc_incoterm_code
                  || vin_icc_incoterm_description
                  || vin_icc_incoterm_name
                  || vin_icc_price
                  || vin_icc_price_cur_sk
                  || vin_icc_price_freght_kb_cur_sk
                  || vin_icc_price_freight
                  || vin_icc_price_freight_cur_sk
                  || vin_icc_price_freight_kb
                  || vin_icc_price_insurance
                  || vin_icc_price_insurance_cur_sk
                  || vin_ignition_key
                  || vin_inco_terms_mc
                  || vin_inco_terms_mme
                  || vin_info_price
                  || vin_info_txt
                  || vin_language_code
                  || vin_language_name
                  || vin_last_cmpnd_unload_cal_sk
                  || vin_latest_eta_cal_sk
                  || vin_lemans_dist_sk
                  || vin_lemans_dlr_sk
                  || vin_lemans_orig_dist_sk
                  || vin_loading_port_name
                  || vin_load_cal_sk
                  || vin_load_for_delivery_cal_sk
                  || vin_load_receive_cal_sk
                  || vin_local_doc_nr
                  || vin_local_doc_prt_date
                  || vin_lpg_flag
                  || vin_lt_arrival_delivered
                  || vin_lt_arrival_load_pdi_cmpd
                  || vin_lt_bill_of_lading_arrival
                  || vin_lt_bill_of_lading_unload
                  || vin_lt_ccd_delivered
                  || vin_lt_doa_received_delivered
                  || vin_lt_doa_received_processed
                  || vin_lt_doa_received_pro_sent
                  || vin_lt_doa_recvd_load_pdi_cmpd
                  || vin_lt_fdo_received_delivered
                  || vin_lt_fdo_received_processed
                  || vin_lt_fdo_received_pro_sent
                  || vin_lt_fdo_recvd_load_pdi_cmpd
                  || vin_lt_fdo_rec_delivered_trgt
                  || vin_lt_fdo_rec_pro_sent_trgt
                  || vin_lt_load_pdi_cmpd_delivered
                  || vin_lt_processed_delivered
                  || vin_lt_processed_load_pdi_cmpd
                  || vin_lt_processed_ready_for_trp
                  || vin_lt_process_delivered_trgt
                  || vin_lt_prodction_load_pdi_cmpd
                  || vin_lt_prodcton_bill_of_lading
                  || vin_lt_pro_sent_delivered
                  || vin_lt_pro_sent_load
                  || vin_lt_pro_sent_processed
                  || vin_lt_pro_sent_process_trgt
                  || vin_lt_ready_for_trp_delivered
                  || vin_lt_ready_trp_load_pdi_cmpd
                  || vin_lt_rft_delivered_trgt
                  || vin_lt_unload_delivered
                  || vin_lt_unload_load_pdi_cmpd
                  || vin_mae_reg_number
                  || vin_maf_cerfa_creation_tst
                  || vin_maf_dereg_demo_tst
                  || vin_maf_reg_crm_cust_id
                  || vin_maf_reg_crm_tenant_id
                  || vin_maf_reg_ext_process_type
                  || vin_maf_reg_last_request_tst
                  || vin_maf_siv_reg_type
                  || vin_manufacturer_name
                  || vin_marketcode
                  || vin_mc_invoice_nr
                  || vin_measurement
                  || vin_mex_port_cal_arrival_sk
                  || vin_mex_salam_cal_departure_sk
                  || vin_mex_trnsprt_to_port_mode
                  || vin_mle_invoice_cal_sk
                  || vin_motor_nr
                  || vin_net_weight
                  || vin_nk
                  || vin_nos_cal_sk
                  || vin_nos_entered_cal_sk
                  || vin_nos_status
                  || vin_nr_of_nos
                  || vin_nr_of_vins
                  || vin_nsc_invoice_cal_sk
                  || vin_order_cal_sk
                  || vin_orig_cmpd_sk
                  || vin_orig_dist_sk
                  || vin_orig_dlr_sk
                  || vin_orig_eta_cal_sk
                  || vin_orig_hold_dlr_sk
                  || vin_orig_mc_production_cal_sk
                  || vin_orig_mc_shipping_cal_sk
                  || vin_orig_mc_vin
                  || vin_or_son
                  || vin_payment_type_code
                  || vin_pdi_cmpd_load_cal_sk
                  || vin_pdi_cmpd_sk
                  || vin_pdi_ready_cal_sk
                  || vin_pdi_start_cal_sk
                  || vin_pfp_incoterm_code
                  || vin_pfp_incoterm_description
                  || vin_pfp_incoterm_name
                  || vin_pfp_price
                  || vin_pfp_price_cur_sk
                  || vin_pip_avail_code
                  || vin_planning_priority_code
                  || vin_planning_priority_descrptn
                  || vin_planning_priority_name
                  || vin_plant_code
                  || vin_plant_country_iso2
                  || vin_plant_country_iso3
                  || vin_plant_country_name
                  || vin_plant_departure_cal_sk
                  || vin_plant_description
                  || vin_plant_name
                  || vin_plnt_local_production_flag
                  || vin_ppln_status_code
                  || vin_ppln_status_description
                  || vin_production_cal_sk
                  || vin_production_line
                  || vin_production_line_code
                  || vin_production_status
                  || vin_prod_cancel_flag
                  || vin_pro_inspection_cal_sk
                  || vin_pro_planned_cal_sk
                  || vin_pro_sent_cal_sk
                  || vin_pur_prc_amt_mme
                  || vin_pur_prc_crcy_mme
                  || vin_rail_wagon_number
                  || vin_ready_for_transport_cal_sk
                  || vin_regional_swap_flag
                  || vin_registration_paper
                  || vin_registration_source
                  || vin_reg_doc_release_tst
                  || vin_reg_number
                  || vin_reg_status
                  || vin_reg_usage
                  || vin_reg_usage_type_code
                  || vin_repair_cal_sk
                  || vin_rsa_sara_provider
                  || vin_sales_code
                  || vin_sales_projection_channel
                  || vin_security_code
                  || vin_service_type
                  || vin_shipment_number
                  || vin_sk
                  || vin_special_sales_description
                  || vin_special_sales_flag
                  || vin_swap_flag
                  || vin_tax_class_code
                  || vin_tax_class_descr
                  || vin_total_ved_amount
                  || vin_transmission_serial_nr
                  || vin_transport_mode_code_cfrmd
                  || vin_transport_partner_sk
                  || vin_unload_cal_sk
                  || vin_vessel_name
                  || vin_vin_comment
                  || vin_vin_owner
                  || vin_visibility_code
                  || vin_visibility_description
                  || vin_visibility_status
                  || vin_voyage_code
                  || vin_warranty_dist_sk
                  || vin_warranty_dlr_sk
                  || vin_warranty_end_cal_sk
                  || vin_warranty_start_cal_sk
                  || vin_whs_funded_bank_id
                  || vin_whs_funded_bus_type
                  || vin_whs_funded_cancel_date
                  || vin_whs_funded_clearing_date
                  || vin_whs_funded_code
                  || vin_whs_funded_description
                  || vin_whs_funded_eot_date
                  || vin_whs_funded_fin_date
                  || vin_whs_funded_intr_free_days
                  || vin_whs_funded_intr_pay_days
                  || vin_whs_funded_int_dlr_date
                  || vin_whs_funded_inv_nr
                  || vin_whs_funded_mazda_credit_id
                  || vin_whs_funded_plan_change_dt
                  || vin_whs_funded_plan_code
                  || vin_whs_funded_settl_date
                  || vin_whs_funded_settl_req_date, 'MD5') hash_data
FROM
    table1 t
WHERE
    ROWNUM <= 100;