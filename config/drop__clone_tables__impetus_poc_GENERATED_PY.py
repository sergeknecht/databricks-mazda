# Databricks notebook source
# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS dev_test.stg.stg_lem_no_working_days

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS dev_test.stg.stg_lem_countries

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS dev_test.stg.iot_stg_dlr_dist_interface

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS dev_test.stg.iot_dim_dist

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS dev_test.stg.stg_dsr_dist_conversion

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS dev_test.stg.stg_dsr_vehicle_master

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS dev_test.stg.stg_dsr_svc_records

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS dev_test.stg.stg_emot_btnregh

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS dev_test.stg.stg_lem_vehicle_distributors

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS dev_test.stg.stg_lem_dealers

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS dev_test.stg.stg_lem_distributors

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS dev_test.stg.stg_mud_interface_code

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS dev_test.stg.iot_dim_dlr_car_delv_address

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS dev_test.stg.iot_dim_dlr

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS dev_test.stg.stg_lem_vehicle_state_flow

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS dev_test.stg.stg_lem_vehicle_state_values

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS dev_test.stg.stg_lem_vehicle_state_types

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS dev_test.stg_tmp.stg_tmp_coc_vins

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS dev_test.stg.stg_emot_btncdhdr

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS dev_test.stg.stg_emot_btnpplnh

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS dev_test.stg.stg_epd_custclassification

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS dev_test.stg.stg_epd_cstclssificationassign

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS dev_test.stg.stg_epd_custassign

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS dev_test.stg.stg_epd_model

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS dev_test.stg.iot_dim_veh_product

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS dev_test.stg.stg_emot_btnihdr

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS dev_test.stg.stg_vin_lem

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS dev_test.stg.stg_lem_damage_cases

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS dev_test.stg.stg_lem_veh_cmpd_information

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS dev_test.stg.stg_lem_compounds

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS dev_test.stg.stg_lem_partner_codes

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS dev_test.stg.stg_lem_vehicle_route_segments

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS dev_test.stg.stg_lem_route_segments

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS dev_test.stg.stg_lem_transport_lines

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS dev_test.stg.stg_lem_transport_line_defs

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS dev_test.stg.stg_lem_outgoing_messages

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS dev_test.stg.stg_lem_vehicles

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS dev_test.stg.stg_lem_vehicle_routes

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS dev_test.stg.stg_lem_routes

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS dev_test.stg.stg_lem_route_defs

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS dev_test.stg.stg_lem_processing_types

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS dev_test.lz_lem.vpr_summary

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS dev_test.stg.stg_lem_vpr_summary

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS dev_test.stg.stg_lem_vpr_types

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS dev_test.stg.stg_lem_outgoing_messages

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS dev_test.stg.stg_lem_transport_modes

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS dev_test.stg.stg_lem_vehicle_origins

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS dev_test.stg.stg_lem_ports

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS dev_test.stg.stg_lem_origins

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS dev_test.stg.stg_lem_voyages

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS dev_test.stg.stg_lem_vessels

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS dev_test.stg.iot_dim_country

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS dev_test.stg.stg_lem_planning_priorities

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS dev_test.stg.stg_lem_inco_terms

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS dev_test.stg.stg_lem_currencies

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS dev_test.stg.iot_dim_cur

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS dev_test.stg.stg_lem_languages

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS dev_test.stg.iot_dim_supply_chain_region

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS dev_test.stg.iot_dim_cmpd

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS dev_test.stg.iot_dim_vin

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS dev_test.stg.stg_lem_vehicle_destinations

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS dev_test.stg.stg_lem_destinations

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS dev_test.stg.stg_lem_vehicle_dist_prices

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS dev_test.stg.stg_lem_rtdam_audit

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS dev_test.stg.stg_lem_plants

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS dev_test.stg.iot_dim_prtn

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS dev_test.stg.stg_veh_master_btv14010

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS dev_test.stg.iot_dim_intr_color

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS dev_test.stg.iot_dim_extr_color

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS dev_test.stg.stg_emot_btnpldel

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS dev_test.stg.stg_emot_btnppln

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS dev_test.stg.stg_emot_btnorech

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS dev_test.stg.stg_emot_btnorec

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS dev_test.stg.stg_siebel_agreement_data

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS dev_test.stg.stg_emot_btncrcar

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS dev_test.stg.stg_emot_btnregfr

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS dev_test.stg.iot_dim_coc_type_approval

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS dev_test.stg.stg_emot_btnregsiv

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS dev_test.stg.stg_emot_btnregext

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS dev_test.stg.stg_emot_btncustcontract

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS dev_test.stg.stg_emot_btnmuser

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS dev_test.stg.stg_emot_btncustomer

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS dev_test.stg.stg_emot_btnreg

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS dev_test.stg.stg_emot_btnbpm

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS dev_test.stg.stg_emot_btnregexdemo

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS dev_test.stg.stg_emot_btncocv

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS dev_test.stg.stg_emot_btnppext

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS dev_test.stg.stg_emot_btnvblfl

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS dev_test.stg.iot_dim_veh_product_local

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS dev_test.stg.iot_stg_emot_btnmprop

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS dev_test.stg.stg_emot_btndoc_events

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS dev_test.stg.stg_tech_document_status

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS dev_test.stg.stg_emot_btnaddr

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS dev_test.stg.stg_emot_btndoc_events_detail

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS dev_test.stg.stg_emot_btnboevt

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS dev_test.stg.stg_emot_btnrprtt

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS dev_test.stg.stg_emot_btnrprt

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS dev_test.stg.stg_dsr_svc_rec_items

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS dev_test.stg.stg_dsr_svc_items

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS dev_test.stg.stg_dsr_rsa_sara_providers

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS dev_test.stg.stg_dsr_vehicle_warranty

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS dev_test.stg.stg_vin_emot

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS dev_test.stg.stg_vin_wrty

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS dev_test.stg.stg_vin_lem_no_working_days

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS dev_test.stg.stg_lem_lead_time_targets

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS dev_test.stg.stg_vin_dsr

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS dev_test.stg.stg_emot_btnprop_xref_rel

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS dev_test.stg.stg_emot_btnprop_xref

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS dev_test2.stg.iot_stg_lem_no_working_days

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS dev_test2.stg.iot_stg_lem_no_working_days_tp

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS dev_test2.stg.iot_stg_dsr_dlr_dist_lkp

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS dev_test2.stg.iot_stg_emot_last_dereg

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS dev_test2.stg.iot_stg_lem_dlr_dist_lkp

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS dev_test2.stg.iot_stg_lem_customs_states

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS dev_test2.stg.iot_stg_emot_load_receive

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS dev_test2.stg.iot_stg_epd_lpg_customization

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS dev_test2.stg.iot_stg_emot_mle_invoice

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS dev_test2.stg.stg_vin_lem_no_working_days

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS dev_test2.stg.iot_stg_emot_nsc_invoice

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS dev_test2.stg.iot_stg_lem_process_states

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS dev_test2.stg.iot_stg_lem_repair_states

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS dev_test2.stg.iot_stg_lem_transport_mexico

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS dev_test2.stg.iot_stg_lem_transport_states

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS dev_test2.stg.iot_stg_lem_vpr_summary

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS dev_test2.stg_tmp.stg_tmp_lem_pdi_rdy_date

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS dev_test2.stg_tmp.stg_tmp_lem_first_tr_unload

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS dev_test2.stg.stg_vin_lem

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS dev_test2.stg_tmp.t__ttl_ppln

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS dev_test2.stg_tmp.t__iot_dim_cmpd

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS dev_test2.stg_tmp.t__first_demo_orec

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS dev_test2.stg.stg_vin_emot

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS dev_test2.stg_tmp.v__temp_200_all_svc

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS dev_test2.stg_tmp.v__temp_210_vhm_info

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS dev_test2.stg.stg_dim_vin

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS dev_test.stg.stg_lem_partners