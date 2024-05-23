SELECT
    DISTINCT da.asset_id AS "Asset ID",
    da.ip_address AS "Asset IP Address",
    da.host_name AS "Asset Name",
    da.mac_address AS "Asset MAC Addresses",
    dt.tag_name AS "Asset Location",
    daua.full_name AS "Asset Owner",
    -- 'TBD' AS "Asset Criticality",
    -- 'TBD' AS "Custom Tag",
    dv.title AS "Vulnerability Title",
    dvr.reference AS "CVE ID",
    dv.nexpose_id AS "CVE ID 2", -- added to check which is correct
    dv.cvss_score AS "CVSS Score",
    dv.riskscore AS "Vulnerability Risk Score",
    fa.riskscore AS "Asset Risk Score",
    dv.description AS "Vulnerability Description",
    fasvi.proof AS "Vulnerability Proof",
    ds.fix AS "Vulnerability Solution",    
    dox.version AS "Asset OS Version",
    dos.description AS "Operating System",
    dox.family AS "Asset OS Family",
    fr.exploits AS "Exploit Count",
    dasc.name AS "Service Name",
    dasc.port AS "Service Port",
    dp.name AS "Service Product Service Protocol",
    fava.age_in_days AS "Vulnerability Age",
    fava.first_discovered AS "First Found Date",
    favi.date AS "Vulnerability Test Date",
    dv.nexpose_id AS "Vulnerability ID",    
    
    -- dos.asset_type AS "Asset Type",
    -- daos.certainty AS "Operating System Certainty",
    -- fava.most_recently_discovered AS "Last Found Date",
    -- dvc.category_name AS "Vulnerability Category",
    -- dvc.category_id AS "Vulnerability Category ID"
FROM dim_asset da
LEFT JOIN fact_asset fa ON da.asset_id = fa.asset_id
LEFT JOIN dim_asset_service_configuration dasc ON da.asset_id = dasc.asset_id
LEFT JOIN dim_asset_service_credential dascr ON da.asset_id = dascr.asset_id
LEFT JOIN dim_protocol dp ON dascr.protocol_id = dp.protocol_id
LEFT JOIN dim_asset_user_account daua ON da.asset_id = daua.asset_id
LEFT JOIN fact_asset_scan_vulnerability_instance fasvi ON da.asset_id = fasvi.asset_id
LEFT JOIN fact_asset_vulnerability_age fava ON fasvi.vulnerability_id = fava.vulnerability_id
LEFT JOIN no_solutions ns ON ns.vulnerability_id = fava.vulnerability_id
LEFT JOIN dim_vulnerability dv ON fasvi.vulnerability_id = dv.vulnerability_id
LEFT JOIN dim_vulnerability_reference dvr ON fasvi.vulnerability_id = dvr.vulnerability_id
LEFT JOIN fact_asset_vulnerability_instance favi ON fasvi.vulnerability_id = favi.vulnerability_id
LEFT JOIN dim_asset_operating_system daos ON da.asset_id = daos.asset_id
LEFT JOIN dim_operating_system dos ON daos.operating_system_id = dos.operating_system_id
LEFT JOIN dim_vulnerability_solution dvs ON dv.vulnerability_id = dvs.solution_id
LEFT JOIN dim_solution ds ON dvs.solution_id = ds.solution_id
LEFT JOIN fact_remediation(2147483647, 'riskscore DESC') fr ON ds.solution_id = fr.solution_id
LEFT JOIN dim_vulnerability_category dvc ON dv.vulnerability_id = dvc.vulnerability_id
LEFT JOIN dim_tag_asset dta ON da.asset_id = dta.asset_id
LEFT JOIN dim_tag dt ON dta.tag_id = dt.tag_id
WHERE dv.cvss_score >= 7
