{{ config(materialized='table') }}

/*
    Hydro catchment dimension. One row per major NZ hydro storage lake.
    Source: hydro_catchment_mapping seed (EMI HMD site codes + metadata).
    Grain: site_code (e.g. NI_TPO, SI_PKI).
*/

SELECT
    {{ dbt_utils.generate_surrogate_key(['site_code']) }} AS catchment_key,
    site_code,
    catchment_name,
    island,
    plant_group,
    scheme
FROM {{ ref('hydro_catchment_mapping') }}
