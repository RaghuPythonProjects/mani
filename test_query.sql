SELECT
    DISTINCT favf.asset_id AS "Asset ID",
    da.ip_address AS "Asset IP Address",
    da.host_name AS "Asset Name",
    dt.tag_name AS "Asset Location",
    dv.title AS "Vulnerability Title",
    dvr.reference AS "CVE ID",
    dv.cvss_score AS "CVSS Score",
    dv.riskscore AS "Vulnerability Risk Score",
    dv.description AS "Vulnerability Description",
    htmlAsText(favf.proof) AS "Vulnerability Proof",
    htmlAsText(ds.fix) AS "Vulnerability Solution",
    dos.description AS "Operating System",
    dos.asset_type AS "Asset Type",
    daos.certainty AS "Operating System Certainty",
    fava.age_in_days AS "Vulnerability Age",
    fava.first_discovered AS "First Found Date",
    fava.most_recently_discovered AS "Last Found Date",
    dvc.category_name AS "Vulnerability Category",
    dvc.category_id AS "Vulnerability Category ID",
    dv.nexpose_id AS "Vulnerability ID"
FROM fact_asset_vulnerability_finding favf
JOIN fact_asset_vulnerability_age fava ON favf.vulnerability_id = fava.vulnerability_id
JOIN dim_asset da ON favf.asset_id = da.asset_id
JOIN dim_vulnerability dv ON favf.vulnerability_id = dv.vulnerability_id
JOIN dim_vulnerability_reference dvr ON favf.vulnerability_id = dvr.vulnerability_id
JOIN dim_asset_operating_system daos ON favf.asset_id = daos.asset_id
JOIN dim_operating_system dos ON daos.operating_system_id = dos.operating_system_id
JOIN dim_fingerprint_source dfs ON daos.fingerprint_source_id = dfs.fingerprint_source_id
JOIN dim_vulnerability_solution dvs ON dv.vulnerability_id = dvs.solution_id
JOIN dim_solution ds ON dvs.solution_id = ds.solution_id
JOIN dim_vulnerability_category dvc ON dv.vulnerability_id = dvc.vulnerability_id
JOIN dim_tag_asset dat ON da.asset_id = dat.asset_id
JOIN dim_tag dt ON dat.tag_id = dt.tag_id
WHERE dv.cvss_score >= 7
