# Databricks notebook source
# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS dev_test.lz_bill.tbcbi_code

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS dev_test.lz_bill.tbcbi_item

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS dev_test.lz_bill.tbcbi_nonstdvatcd

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS dev_test.lz_bill.tbmjy_parokey

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS dev_test.lz_biut.bifileupload_versiondesc

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS dev_test.lz_biut.cause

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS dev_test.lz_biut.client

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS dev_test.lz_biut.market

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS dev_test.lz_biut.marketdistributorcountry

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS dev_test.lz_biut.marketmapping

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS dev_test.lz_biut.marketstructure

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS dev_test.lz_biut.markettype

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS dev_test.lz_biut.pick_pack_check_result

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS dev_test.lz_biut.pick_pack_location

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS dev_test.lz_biut.pick_pack_status

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS dev_test.lz_biut.workingday

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS dev_test.lz_coc.tbcoc_product

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS dev_test.lz_coc.tbcoc_regexchg

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS dev_test.lz_coc.tbcoc_regexcode

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS dev_test.lz_coc.tbcoc_regexfield

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS dev_test.lz_coc.tbcoc_status

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS dev_test.lz_coc.tbcoc_value

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS dev_test.lz_cts2.application

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS dev_test.lz_cts2.key

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS dev_test.lz_cts2.label

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS dev_test.lz_cts2.language_locale

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS dev_test.lz_dsr.access_log

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS dev_test.lz_dsr.checklist

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS dev_test.lz_dsr.check_attachment_ref

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS dev_test.lz_dsr.check_location_result

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS dev_test.lz_dsr.check_reminder

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS dev_test.lz_dsr.check_result

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS dev_test.lz_dsr.check_result_detail

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS dev_test.lz_dsr.defect

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS dev_test.lz_dsr.dsb_country_params

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS dev_test.lz_dsr.dsb_dist_nsc_conversion

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS dev_test.lz_dsr.external_contracts

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS dev_test.lz_dsr.ext_svc_plans

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS dev_test.lz_dsr.ext_svc_plan_items

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS dev_test.lz_dsr.map_updates

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS dev_test.lz_dsr.mv_rptbonus_detail

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS dev_test.lz_dsr.mv_rptbonus_detail_roll

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS dev_test.lz_dsr.mv_rptbonus_summary_dlr

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS dev_test.lz_dsr.mv_rptbonus_summary_dlr_roll

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS dev_test.lz_dsr.mv_rptbonus_summary_nsc

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS dev_test.lz_dsr.mv_rptbonus_summary_nsc_roll

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS dev_test.lz_dsr.mv_vehicle_warranty

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS dev_test.lz_dsr.nsc_dlr_conversions

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS dev_test.lz_dsr.oersa_contracts

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS dev_test.lz_dsr.oew_contracts

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS dev_test.lz_dsr.offerli_attref_relation

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS dev_test.lz_dsr.offer_attref_relation

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS dev_test.lz_dsr.rsa_sara_providers

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS dev_test.lz_dsr.svc_records

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS dev_test.lz_dsr.svc_rec_items

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS dev_test.lz_dsr.svc_rec_items_id

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS dev_test.lz_dsr.vehicle_master

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS dev_test.lz_dsr.vin_location

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS dev_test.lz_dsr.wo_transmissions_curr

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS dev_test.lz_dsr.w_invoices

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS dev_test.lz_dsr.w_invoices_curr

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS dev_test.lz_dsr.w_invoice_items

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS dev_test.lz_dsr.w_invoice_items_curr

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS dev_test.lz_dsr.w_orders

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS dev_test.lz_dsr.w_orders_curr

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS dev_test.lz_dsr.w_order_items

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS dev_test.lz_dsr.w_order_items_curr

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS dev_test.lz_dsr.w_order_svc_items_curr

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS dev_test.lz_ebonus.mv_dwh_calculation_current

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS dev_test.lz_eclaims.claim

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS dev_test.lz_eclaims.claim_decision

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS dev_test.lz_eclaims.external_advice

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS dev_test.lz_emot.btnboevt

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS dev_test.lz_emot.btnbpm

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS dev_test.lz_emot.btncocv

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS dev_test.lz_emot.btncrcar

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS dev_test.lz_emot.btncrinfh

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS dev_test.lz_emot.btncustcontract

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS dev_test.lz_emot.btncustomer

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS dev_test.lz_emot.btndeal

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS dev_test.lz_emot.btndeal_audit

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS dev_test.lz_emot.btndoc_events

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS dev_test.lz_emot.btndoc_events_detail

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS dev_test.lz_emot.btnihdr

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS dev_test.lz_emot.btnmuser

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS dev_test.lz_emot.btnordform

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS dev_test.lz_emot.btnordrq

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS dev_test.lz_emot.btnorec

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS dev_test.lz_emot.btnorech

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS dev_test.lz_emot.btnorpnd

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS dev_test.lz_emot.btnpcst

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS dev_test.lz_emot.btnpdisc

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS dev_test.lz_emot.btnpdsc

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS dev_test.lz_emot.btnpgpc

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS dev_test.lz_emot.btnplnmv

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS dev_test.lz_emot.btnppln

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS dev_test.lz_emot.btnpplnh

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS dev_test.lz_emot.btnprocresult

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS dev_test.lz_emot.btnprop_xref

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS dev_test.lz_emot.btnprop_xref_rel

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS dev_test.lz_emot.btnreg

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS dev_test.lz_emot.btnregexdemo

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS dev_test.lz_emot.btnregext

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS dev_test.lz_emot.btnregfr

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS dev_test.lz_emot.btnregh

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS dev_test.lz_emot.btnregsiv

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS dev_test.lz_emot.btnrprtt

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS dev_test.lz_emot.btnvinh

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS dev_test.lz_epd.tbbas_disassign

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS dev_test.lz_epd.tbbas_distribut

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS dev_test.lz_epd.tbbas_lanassign

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS dev_test.lz_epd.tbbas_language

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS dev_test.lz_epd.tbepd_basecode

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS dev_test.lz_epd.tbepd_colassign

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS dev_test.lz_epd.tbepd_colour

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS dev_test.lz_epd.tbepd_custassign

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS dev_test.lz_epd.tbepd_custclassification

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS dev_test.lz_epd.tbepd_custclassificationassign

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS dev_test.lz_epd.tbepd_eleassign

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS dev_test.lz_epd.tbepd_element

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS dev_test.lz_epd.tbepd_gvscarlinemodelassign

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS dev_test.lz_epd.tbepd_model

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS dev_test.lz_epd.tbepd_mscassign

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS dev_test.lz_epd.tbepd_pluflag

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS dev_test.lz_epd.tbepd_pluflaghistory

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS dev_test.lz_epd.tbepd_series

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS dev_test.lz_epss.attachment

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS dev_test.lz_epss.checksheet_type

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS dev_test.lz_epss.codetype

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS dev_test.lz_epss.ignitionkey_log

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS dev_test.lz_epss.immobilizer_log

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS dev_test.lz_epss.pqi

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS dev_test.lz_epss.pqi_cvc

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS dev_test.lz_epss.pqi_dtc

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS dev_test.lz_epss.pqi_history

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS dev_test.lz_epss.reqmaster

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS dev_test.lz_epss.reqmaster_history

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS dev_test.lz_epss.reqmaster_part

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS dev_test.lz_epss.reqmaster_requestor

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS dev_test.lz_epss.tc

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS dev_test.lz_epss.tc_history

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS dev_test.lz_epss.tc_model

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS dev_test.lz_eqt.bi_offer_event

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS dev_test.lz_eqt.bi_offer_event_accessory

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS dev_test.lz_eqt.bi_offer_event_used_car

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS dev_test.lz_eqt.bi_offer_event_veh_disc

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS dev_test.lz_eqt.bi_sales_contract

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS dev_test.lz_etas.btgaddresses

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS dev_test.lz_etas.btgcompanies

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS dev_test.lz_etas.btgemployment_status

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS dev_test.lz_etas.btgfunctions

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS dev_test.lz_etas.btgjobroles

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS dev_test.lz_etas.btguserfunctions

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS dev_test.lz_etas.btgusers

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS dev_test.lz_etas.btguser_details

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS dev_test.lz_file.cemi

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS dev_test.lz_file.cip_customer

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS dev_test.lz_file.cip_master_customer

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS dev_test.lz_file.rdc_cancellations_daily

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS dev_test.lz_file.rdc_order

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS dev_test.lz_file.rdc_order_daily

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS dev_test.lz_file.rdc_pickup_daily

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS dev_test.lz_fms.accessories

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS dev_test.lz_fms.accountingcategory_mappings

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS dev_test.lz_fms.cars

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS dev_test.lz_fms.carstates

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS dev_test.lz_fms.car_calculations

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS dev_test.lz_fms.car_licenseplate

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS dev_test.lz_fms.car_types

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS dev_test.lz_fms.currencies

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS dev_test.lz_fms.distances

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS dev_test.lz_fms.distanceunits

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS dev_test.lz_fms.invoices

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS dev_test.lz_fms.licenseplates

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS dev_test.lz_fms.models

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS dev_test.lz_fms.netamounts

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS dev_test.lz_fms.online_states

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS dev_test.lz_fms.orders

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS dev_test.lz_fms.precarorders

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS dev_test.lz_fms.prices

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS dev_test.lz_fms.usages

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS dev_test.lz_fms.usage_types

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS dev_test.lz_ga.dwr_page_views

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS dev_test.lz_ga.monthly_web_report

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS dev_test.lz_ga.mwr_campaign_goals

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS dev_test.lz_ga.mwr_sessions

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS dev_test.lz_ga.mwr_source_medium

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS dev_test.lz_ga.mwr_url_category_page

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS dev_test.lz_ga.mwr_url_landing_page

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS dev_test.lz_ga.website_tracking

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS dev_test.lz_imc.category_composing

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS dev_test.lz_imc.category_description

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS dev_test.lz_imc.e_component

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS dev_test.lz_imc.e_componenttype

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS dev_test.lz_imc.e_component_curr

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS dev_test.lz_imc.e_description

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS dev_test.lz_imc.e_description_curr

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS dev_test.lz_imc.learningform_d

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS dev_test.lz_imc.participant_prebooking_curr

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS dev_test.lz_imc.person

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS dev_test.lz_imc.person_component_assignment

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS dev_test.lz_imc.person_curr

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS dev_test.lz_imc.person_custom

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS dev_test.lz_imc.portfolio

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS dev_test.lz_imc.portfolio_curr

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS dev_test.lz_imc.r_location_structure_d

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS dev_test.lz_imc.topic_composing

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS dev_test.lz_imc.topic_composing_ids

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS dev_test.lz_imc.topic_description

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS dev_test.lz_imc.topic_root_d

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS dev_test.lz_inc.dmm_dealer_settings

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS dev_test.lz_inc.dmm_match_explanations

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS dev_test.lz_inc.dmm_model_group

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS dev_test.lz_inc.dmm_model_group_carline

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS dev_test.lz_inc.dmm_model_group_target_range

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS dev_test.lz_inc.dmm_pos_dlr_sttngs_mgr

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS dev_test.lz_inc.dmm_result

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS dev_test.lz_inc.dmm_sanctioning

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS dev_test.lz_inc.dmm_sanctioning_result

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS dev_test.lz_inc.dmm_tdefi

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS dev_test.lz_inc.dmm_vin_data

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS dev_test.lz_inc.dmm_vin_result

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS dev_test.lz_inc.tbinc_campaign_car

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS dev_test.lz_inc.tbinc_cond

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS dev_test.lz_inc.tbinc_cpntype

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS dev_test.lz_inc.tbinc_emotivmirror

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS dev_test.lz_inc.tbinc_emotivmirror_invoice

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS dev_test.lz_inc.tbinc_finzinf

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS dev_test.lz_inc.tbinc_match

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS dev_test.lz_inc.tbinc_result

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS dev_test.lz_inc.tbinc_tdefi

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS dev_test.lz_inc.tbinc_vinpayment

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS dev_test.lz_lem.cost_details

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS dev_test.lz_lem.cost_details_curr

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS dev_test.lz_lem.ddn_rtdam_audit

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS dev_test.lz_lem.ddn_vehicle_distributors

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS dev_test.lz_lem.ddn_vehicle_state_flow

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS dev_test.lz_lem.mpi_invoices

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS dev_test.lz_lem.rtdam_audit

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS dev_test.lz_lem.vehicles

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS dev_test.lz_lem.vehicle_accessories

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS dev_test.lz_lem.vehicle_destinations

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS dev_test.lz_lem.vehicle_distributors

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS dev_test.lz_lem.vehicle_origins

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS dev_test.lz_lem.vehicle_process_statuses

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS dev_test.lz_lem.vehicle_route_segments

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS dev_test.lz_lem.vehicle_state_flow

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS dev_test.lz_lem.vpr_sent

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS dev_test.lz_lem.vpr_summary

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS dev_test.lz_mud.bi_dealer

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS dev_test.lz_mud.bi_dealer_code_history

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS dev_test.lz_mud.bi_dealer_group

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS dev_test.lz_mud.bi_dealer_service

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS dev_test.lz_mud.bi_distributor

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS dev_test.lz_mud.bi_interface_code

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS dev_test.lz_mud.bi_parts_distributor_service

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS dev_test.lz_mud.bi_relation

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS dev_test.lz_mud.bi_salestarget

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS dev_test.lz_mud.bi_salestargetproposal

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS dev_test.lz_mum.tuser

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS dev_test.lz_ngeps.all_part_flags

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS dev_test.lz_ngeps.classification_hist

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS dev_test.lz_ngeps.mv_classification

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS dev_test.lz_ngeps.v_campaign

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS dev_test.lz_ngeps.v_cif_antwerp

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS dev_test.lz_ngeps.v_dealer_net

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS dev_test.lz_ngeps.v_fob_brussels

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS dev_test.lz_ngeps.v_fob_japan

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS dev_test.lz_ngeps.v_master_retail

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS dev_test.lz_ngeps.v_part_flags

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS dev_test.lz_ngeps.v_simulation

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS dev_test.lz_ngeps.v_transfer_cif

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS dev_test.lz_osb.change_log_files

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS dev_test.lz_osb.event_details_files

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS dev_test.lz_osb.login_files

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS dev_test.lz_otm.ie_shipmentstatus

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS dev_test.lz_otm.shipment

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS dev_test.lz_otm.xxspe_shipmentstatus_v

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS dev_test.lz_pa.btsbdytp

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS dev_test.lz_pa.btscarln

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS dev_test.lz_pa.btsclaim_hist00

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS dev_test.lz_pa.btscldlr_hist00

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS dev_test.lz_pa.btscllog_hist00

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS dev_test.lz_pa.btsctcrl

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS dev_test.lz_pa.btsdldch

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS dev_test.lz_pa.btsdlrdt_hist00

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS dev_test.lz_pa.btsdlrgn_mud

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS dev_test.lz_pa.btsdlrgn_mud_hist00

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS dev_test.lz_pa.btsdlrhd_hist00

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS dev_test.lz_pa.btsdlrrh

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS dev_test.lz_pa.btsdlrrt

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS dev_test.lz_pa.btsdsilg

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS dev_test.lz_pa.btseng

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS dev_test.lz_pa.btshstcf

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS dev_test.lz_pa.btshstfb

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS dev_test.lz_pa.btshstfj

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS dev_test.lz_pa.btsmkplt

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS dev_test.lz_pa.btsmkr00

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS dev_test.lz_pa.btsmkrlt

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS dev_test.lz_pa.btsmodel

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS dev_test.lz_pa.btsmrkt

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS dev_test.lz_pa.btspam00

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS dev_test.lz_pa.btspam00_hist

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS dev_test.lz_pa.btspam10

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS dev_test.lz_pa.btspamlt

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS dev_test.lz_pa.btspdt00_hist00

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS dev_test.lz_pa.btsphd00_hist00

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS dev_test.lz_pa.btspli00

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS dev_test.lz_pa.btspmplt

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS dev_test.lz_pa.btspscsp

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS dev_test.lz_pa.btspscsp_sync

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS dev_test.lz_pa.btsreblt

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS dev_test.lz_pa.btsretau_hist00

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS dev_test.lz_pa.btssecst

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS dev_test.lz_pa.btsserie

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS dev_test.lz_pa.btstrm

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS dev_test.lz_pa.bts_mag_hisretail

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS dev_test.lz_pa.bts_mag_retail

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS dev_test.lz_pa.bts_pdt_cancel

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS dev_test.lz_pa.bts_prdc_moq

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS dev_test.lz_pa.pa_campg_id

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS dev_test.lz_pa.pa_rtldv_id

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS dev_test.lz_pa.rb_order_audit

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS dev_test.lz_pim.xxrecon_rep_item_classif_tl_v

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS dev_test.lz_pim.xxrecon_rep_item_lang_trans_v

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS dev_test.lz_protime.absences

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS dev_test.lz_protime.anomalies

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS dev_test.lz_protime.anomaly_types

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS dev_test.lz_protime.clockings

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS dev_test.lz_protime.employees

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS dev_test.lz_protime.shifts

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS dev_test.lz_siebel_oltp.cx_con_segments_curr

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS dev_test.lz_siebel_oltp.cx_merge_record

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS dev_test.lz_siebel_oltp.cx_merge_record_curr

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS dev_test.lz_siebel_oltp.cx_term_cond

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS dev_test.lz_siebel_oltp.cx_term_cond_bu

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS dev_test.lz_siebel_oltp.cx_term_contact

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS dev_test.lz_siebel_oltp.cx_term_lov

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS dev_test.lz_siebel_oltp.s_addr_per

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS dev_test.lz_siebel_oltp.s_addr_per_curr

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS dev_test.lz_siebel_oltp.s_asset

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS dev_test.lz_siebel_oltp.s_asset_accnt

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS dev_test.lz_siebel_oltp.s_asset_atx

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS dev_test.lz_siebel_oltp.s_asset_bu

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS dev_test.lz_siebel_oltp.s_asset_con

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS dev_test.lz_siebel_oltp.s_asset_con_curr

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS dev_test.lz_siebel_oltp.s_asset_curr

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS dev_test.lz_siebel_oltp.s_asset_x

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS dev_test.lz_siebel_oltp.s_camp_call_lst_curr

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS dev_test.lz_siebel_oltp.s_camp_con

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS dev_test.lz_siebel_oltp.s_camp_con_curr

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS dev_test.lz_siebel_oltp.s_communication

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS dev_test.lz_siebel_oltp.s_communication_curr

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS dev_test.lz_siebel_oltp.s_comm_dtl

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS dev_test.lz_siebel_oltp.s_comm_svy_log

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS dev_test.lz_siebel_oltp.s_contact

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS dev_test.lz_siebel_oltp.s_contact_atx_curr

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS dev_test.lz_siebel_oltp.s_contact_bu

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS dev_test.lz_siebel_oltp.s_contact_curr

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS dev_test.lz_siebel_oltp.s_contact_fnx

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS dev_test.lz_siebel_oltp.s_contact_fnx_curr

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS dev_test.lz_siebel_oltp.s_contact_t_curr

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS dev_test.lz_siebel_oltp.s_contact_x

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS dev_test.lz_siebel_oltp.s_contact_x_curr

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS dev_test.lz_siebel_oltp.s_con_addr

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS dev_test.lz_siebel_oltp.s_con_addr_curr

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS dev_test.lz_siebel_oltp.s_con_sm_attr_curr

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS dev_test.lz_siebel_oltp.s_con_sm_prof

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS dev_test.lz_siebel_oltp.s_con_sm_prof_curr

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS dev_test.lz_siebel_oltp.s_doc_agree

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS dev_test.lz_siebel_oltp.s_evt_act

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS dev_test.lz_siebel_oltp.s_evt_act_curr

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS dev_test.lz_siebel_oltp.s_lang

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS dev_test.lz_siebel_oltp.s_mktsvy_ques

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS dev_test.lz_siebel_oltp.s_note_sr

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS dev_test.lz_siebel_oltp.s_note_sr_curr

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS dev_test.lz_siebel_oltp.s_opty

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS dev_test.lz_siebel_oltp.s_opty_asset

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS dev_test.lz_siebel_oltp.s_opty_asset_curr

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS dev_test.lz_siebel_oltp.s_opty_con

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS dev_test.lz_siebel_oltp.s_opty_curr

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS dev_test.lz_siebel_oltp.s_opty_org

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS dev_test.lz_siebel_oltp.s_opty_org_curr

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS dev_test.lz_siebel_oltp.s_opty_src

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS dev_test.lz_siebel_oltp.s_opty_src_curr

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS dev_test.lz_siebel_oltp.s_opty_utx

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS dev_test.lz_siebel_oltp.s_opty_utx_curr

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS dev_test.lz_siebel_oltp.s_opty_x

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS dev_test.lz_siebel_oltp.s_opty_x_curr

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS dev_test.lz_siebel_oltp.s_order

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS dev_test.lz_siebel_oltp.s_order_bu

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS dev_test.lz_siebel_oltp.s_order_item

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS dev_test.lz_siebel_oltp.s_org_bu

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS dev_test.lz_siebel_oltp.s_org_bu_curr

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS dev_test.lz_siebel_oltp.s_org_ext

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS dev_test.lz_siebel_oltp.s_org_ext2_fnx_curr

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS dev_test.lz_siebel_oltp.s_org_ext_curr

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS dev_test.lz_siebel_oltp.s_org_ext_fnx

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS dev_test.lz_siebel_oltp.s_org_ext_fnx_curr

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS dev_test.lz_siebel_oltp.s_org_ext_lsx

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS dev_test.lz_siebel_oltp.s_org_ext_x

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS dev_test.lz_siebel_oltp.s_org_ext_x_curr

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS dev_test.lz_siebel_oltp.s_org_make

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS dev_test.lz_siebel_oltp.s_org_prtnr

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS dev_test.lz_siebel_oltp.s_org_prtnr_curr

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS dev_test.lz_siebel_oltp.s_ou_prtnr_tier

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS dev_test.lz_siebel_oltp.s_party

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS dev_test.lz_siebel_oltp.s_party_curr

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS dev_test.lz_siebel_oltp.s_party_per

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS dev_test.lz_siebel_oltp.s_party_per_curr

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS dev_test.lz_siebel_oltp.s_postn

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS dev_test.lz_siebel_oltp.s_prod_int

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS dev_test.lz_siebel_oltp.s_prod_int_curr

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS dev_test.lz_siebel_oltp.s_revn

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS dev_test.lz_siebel_oltp.s_revn_curr

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS dev_test.lz_siebel_oltp.s_sales_method

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS dev_test.lz_siebel_oltp.s_srv_req

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS dev_test.lz_siebel_oltp.s_srv_req_bu

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS dev_test.lz_siebel_oltp.s_srv_req_curr

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS dev_test.lz_siebel_oltp.s_srv_req_x

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS dev_test.lz_siebel_oltp.s_srv_req_xm

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS dev_test.lz_siebel_oltp.s_srv_req_xm_curr

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS dev_test.lz_siebel_oltp.s_srv_req_x_curr

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS dev_test.lz_siebel_oltp.s_sr_att

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS dev_test.lz_siebel_oltp.s_user

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS dev_test.lz_siebel_oltp.s_user_curr

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS dev_test.lz_siebel_oltp.s_vhcl_fin_dtl

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS dev_test.lz_siebel_oltp.s_vhcl_sales

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS dev_test.lz_siebel_oltp.s_vhcl_sales_bu

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS dev_test.lz_siebel_oltp.s_vhcl_sales_bu_curr

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS dev_test.lz_siebel_oltp.s_vhcl_sales_curr

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS dev_test.lz_siebel_oltp.s_vhcl_srv

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS dev_test.lz_siebel_oltp.s_vhcl_srv_curr

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS dev_test.lz_siebel_oltp.s_vhcl_srv_xm

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS dev_test.lz_sunbil.tbsdb_dict

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS dev_test.lz_sunbil.tbsdb_sunbilhea

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS dev_test.lz_sunbil.tbsdb_sunbilpos

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS dev_test.lz_tsc.tsc_assignment

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS dev_test.lz_tsc.tsc_assignment_id

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS dev_test.lz_tsc.tsc_desc

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS dev_test.lz_tsc.tsc_id

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS dev_test.lz_tsc.tsc_variant_id

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS dev_test.lz_tsc.tsc_variant_nsc_setting_id

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS dev_test.lz_ucm.ucmactions

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS dev_test.lz_ucm.ucmcarsmapped

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS dev_test.lz_ucm.ucmdates

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS dev_test.lz_ucm.ucmfueltypes

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS dev_test.lz_ucm.ucmimages

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS dev_test.lz_ucm.ucminterfaces

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS dev_test.lz_ucm.ucmmapping

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS dev_test.lz_ucm.ucmmappingtypes

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS dev_test.lz_ucm.ucmprices

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS dev_test.lz_ucm.ucmproviders

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS dev_test.lz_ud.organisations

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS dev_test.lz_ud.users

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS dev_test.lz_veh_master.btv14010

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS dev_test.lz_vpo.tbvpo_chg_order

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS dev_test.lz_vpo.tbvpo_ordaddinf

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS dev_test.lz_vpo.tbvpo_order

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS dev_test.lz_vpo.tbvpo_orderrole

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS dev_test.lz_vpo.tbvpo_order_typ

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS dev_test.lz_vpo.tbvpo_prod_stat

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS dev_test.lz_vpo.tbvpo_stage_vpostatus

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS dev_test.lz_vpo.tbvpo_status

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS dev_test.lz_wml.booking

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS dev_test.lz_wml.claim

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS dev_test.lz_wml.operation

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS dev_test.lz_wml.part

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS dev_test.lz_wml.procstep

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS dev_test.lz_wml.sublet

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS dev_test.lz_wml.sublocation

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS dev_test.lz_wml.wml_claim_adjustment_id

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS dev_test.lz_wml.wml_claim_id
