import json

queries = {
    "Total_Waste_Throughout":
        """
        WITH release_totals AS (
            SELECT 
                trf.tri_facility_id,
                SUM(tft.total_offsite_release::NUMERIC + tft.total_onsite_release::NUMERIC) AS Total_Release
            FROM tri_form_total tft
            JOIN tri_reporting_form trf ON tft.doc_ctrl_num = trf.doc_ctrl_num
            GROUP BY trf.tri_facility_id
        )
        SELECT
            MAX(tfh.name) as name,
            ROUND(rt.Total_Release, 2) AS Total_Release,
            rt.tri_facility_id,
            tfh.create_date

        FROM release_totals rt
        JOIN tri_facility_history tfh ON rt.tri_facility_id = tfh.tri_facility_id
        GROUP BY rt.tri_facility_id, tfh.create_date, Total_Release
        ORDER BY tfh.create_date;
        """
    ,
    "Top_10_Facility_Waster_History":
    """
    WITH release_totals AS (
        SELECT 
            trf.tri_facility_id,
            SUM(tft.total_offsite_release::NUMERIC + tft.total_onsite_release::NUMERIC) AS Total_Release
        FROM tri_form_total tft
        JOIN tri_reporting_form trf ON tft.doc_ctrl_num = trf.doc_ctrl_num
        GROUP BY trf.tri_facility_id
    )
    SELECT
        MAX(tfh.name) as name,
        ROUND(SUM(rt.Total_Release), 2) AS Total_Release,
        rt.tri_facility_id

    FROM release_totals rt
    JOIN tri_facility_history tfh ON rt.tri_facility_id = tfh.tri_facility_id
    GROUP BY rt.tri_facility_id
    ORDER BY Total_Release DESC LIMIT 10;
    """,
    "Top_10_Facility_Waster_2020":
    """
    WITH release_totals AS (
        SELECT 
            trf.tri_facility_id,
            SUM(tft.total_offsite_release::NUMERIC + tft.total_onsite_release::NUMERIC) AS Total_Release
        FROM tri_form_total tft
        JOIN tri_reporting_form trf ON tft.doc_ctrl_num = trf.doc_ctrl_num
        GROUP BY trf.tri_facility_id
    )
    SELECT
        tfh.city,
        tfh.county,
        tfh.state,
        ROUND(SUM(rt.Total_Release), 2) AS Total_Release
    FROM release_totals rt
    JOIN tri_facility_history tfh ON rt.tri_facility_id = tfh.tri_facility_id
    GROUP BY tfh.city, tfh.county, tfh.state
    ORDER BY Total_Release DESC;
    """       
}

for x in queries:
    queries[x] = queries[x].replace('\n', ' ')
    queries[x] = " ".join(queries[x].split())
with open('queries.json', 'w') as f:
    json.dump(queries, f, indent=4)