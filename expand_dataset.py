#!/usr/bin/env python3
"""
Expand CPT Medical Billing Dataset
====================================
Loads 15 patients, 20 CPT codes, 30 visits, and 70+ procedure lines
across multiple diseases, diagnoses (ICD-10), and payer types.

Run:  python expand_dataset.py
"""
import os, sys
import snowflake.connector
from datetime import date

def get_conn():
    return snowflake.connector.connect(
        account=os.environ["SNOWFLAKE_ACCOUNT"].replace(".snowflakecomputing.com", ""),
        user=os.environ["SNOWFLAKE_USER"],
        authenticator="programmatic_access_token",
        token=os.environ["SNOWFLAKE_TOKEN"],
        warehouse="COMPUTE_WH",
        database="cpt_demo",
        schema="medical",
    )

# ─────────────────────────────────────────────────────────────────────────────
# 1. CPT CODE MASTER (20 codes across 7 categories)
# ─────────────────────────────────────────────────────────────────────────────
CPT_CODES = [
    # Evaluation & Management
    ("99213", "Office visit, established patient, moderate complexity",        "Evaluation & Management", 120.00),
    ("99214", "Office visit, established patient, high complexity",            "Evaluation & Management", 180.00),
    ("99203", "Office visit, new patient, moderate complexity",                "Evaluation & Management", 150.00),
    ("99395", "Preventive visit, established, age 18-39",                     "Evaluation & Management", 185.00),
    ("99396", "Preventive visit, established, age 40-64",                     "Evaluation & Management", 210.00),
    ("99397", "Preventive visit, established, age 65+",                       "Evaluation & Management", 240.00),
    # Lab
    ("80053", "Comprehensive metabolic panel",                                 "Lab",                      45.00),
    ("85025", "Complete blood count with differential",                        "Lab",                      35.00),
    ("83036", "Hemoglobin A1c",                                                "Lab",                      30.00),
    ("36415", "Blood draw, venipuncture",                                      "Lab",                      25.00),
    # Cardiology / Radiology
    ("93000", "Electrocardiogram (ECG), routine",                              "Cardiology",               85.00),
    ("93306", "Echocardiography with Doppler",                                 "Cardiology",              450.00),
    ("71046", "Chest X-ray, 2 views",                                          "Radiology",                95.00),
    # Vaccine / Administration (existing + new)
    ("90686", "Influenza vaccine, quadrivalent, IM",                           "Vaccine",                  40.00),
    ("90739", "Hepatitis B vaccine, adult, IM",                                "Vaccine",                  65.00),
    ("90471", "Immunization administration, first injection",                  "Administration",            25.00),
    # Pulmonology / Mental Health
    ("94640", "Pressurized or nonpressurized inhalation treatment (nebulizer)","Pulmonology",              55.00),
    ("90837", "Psychotherapy, 60 minutes",                                     "Mental Health",           150.00),
    # Injection / Hospital
    ("J0696", "Ceftriaxone sodium, per 500 mg",                                "Injection",                38.00),
    ("99232", "Subsequent hospital care, moderate complexity",                 "Hospital",                140.00),
]

# ─────────────────────────────────────────────────────────────────────────────
# 2. PATIENTS (15 — mix of ages, genders, insured/self-pay)
# ─────────────────────────────────────────────────────────────────────────────
PATIENTS = [
    # id,           first,       last,        dob,           age, gender, insurance_id
    ("PAT-001", "James",       "Carter",      "1994-03-15",  30, "Male",   "INS-78432901"),
    ("PAT-002", "Sarah",       "Mitchell",    "1979-07-22",  45, "Female", "INS-34521876"),
    ("PAT-003", "Robert",      "Johnson",     "1962-11-05",  62, "Male",   "INS-MCR-00341"),
    ("PAT-004", "Emily",       "Chen",        "1996-04-18",  28, "Female", None),           # self-pay
    ("PAT-005", "Michael",     "Torres",      "1969-09-30",  55, "Male",   "INS-45678234"),
    ("PAT-006", "Linda",       "Williams",    "1957-02-14",  67, "Female", "INS-MCR-00892"),
    ("PAT-007", "David",       "Brown",       "1986-06-25",  38, "Male",   None),           # self-pay
    ("PAT-008", "Maria",       "Garcia",      "1972-12-03",  52, "Female", "INS-88765432"),
    ("PAT-009", "Thomas",      "Anderson",    "1953-08-19",  71, "Male",   "INS-MCR-01234"),
    ("PAT-010", "Jennifer",    "Lee",         "1990-01-11",  34, "Female", "INS-23456789"),
    ("PAT-011", "Christopher", "Wilson",      "1976-05-07",  48, "Male",   None),           # self-pay
    ("PAT-012", "Patricia",    "Martinez",    "1965-10-28",  59, "Female", "INS-56789012"),
    ("PAT-013", "Daniel",      "Thompson",    "1999-03-02",  25, "Male",   "INS-67890123"),
    ("PAT-014", "Nancy",       "White",       "1961-09-16",  63, "Female", "INS-78901234"),
    ("PAT-015", "Kevin",       "Harris",      "1983-07-04",  41, "Male",   "INS-89012345"),
]

# ─────────────────────────────────────────────────────────────────────────────
# 3. VISITS (30 visits, various diseases / ICD-10 codes)
# ─────────────────────────────────────────────────────────────────────────────
VISITS = [
    # id,       pat_id,    date,         type,               provider,              npi,         diag,     notes
    ("VIS-001","PAT-001","2024-06-10","Preventive",        "Dr. Sarah Nguyen",   "1234567890","Z00.00","Annual physical"),
    ("VIS-002","PAT-002","2024-06-15","Sick Visit",        "Dr. James Park",     "2345678901","E11.9", "Type 2 diabetes follow-up"),
    ("VIS-003","PAT-002","2024-09-20","Sick Visit",        "Dr. James Park",     "2345678901","E11.65","Diabetes with hyperglycemia"),
    ("VIS-004","PAT-003","2024-06-18","Follow-up",         "Dr. Sarah Nguyen",   "1234567890","I10",  "Hypertension management"),
    ("VIS-005","PAT-003","2024-10-05","Follow-up",         "Dr. Sarah Nguyen",   "1234567890","I25.10","Coronary artery disease"),
    ("VIS-006","PAT-004","2024-07-02","Sick Visit",        "Dr. Maria Lopez",    "3456789012","J18.9","Pneumonia"),
    ("VIS-007","PAT-005","2024-07-14","Preventive",        "Dr. James Park",     "2345678901","Z00.00","Annual physical 40-64"),
    ("VIS-008","PAT-005","2024-11-01","Follow-up",         "Dr. James Park",     "2345678901","E78.5","Hyperlipidemia management"),
    ("VIS-009","PAT-006","2024-07-22","Preventive",        "Dr. Sarah Nguyen",   "1234567890","Z00.00","Medicare wellness visit 65+"),
    ("VIS-010","PAT-006","2024-11-10","Sick Visit",        "Dr. Sarah Nguyen",   "1234567890","I10",  "Hypertension review"),
    ("VIS-011","PAT-007","2024-08-05","Sick Visit",        "Dr. Maria Lopez",    "3456789012","M54.50","Low back pain"),
    ("VIS-012","PAT-007","2024-10-12","Follow-up",         "Dr. Maria Lopez",    "3456789012","M54.50","Back pain follow-up"),
    ("VIS-013","PAT-008","2024-08-14","Follow-up",         "Dr. James Park",     "2345678901","K21.0","GERD follow-up"),
    ("VIS-014","PAT-008","2024-12-03","Sick Visit",        "Dr. James Park",     "2345678901","E11.9","Diabetes check"),
    ("VIS-015","PAT-009","2024-08-28","Preventive",        "Dr. Sarah Nguyen",   "1234567890","Z00.00","Medicare annual wellness"),
    ("VIS-016","PAT-009","2024-11-20","Follow-up",         "Dr. Sarah Nguyen",   "1234567890","I25.10","Cardiac follow-up"),
    ("VIS-017","PAT-010","2024-09-03","Sick Visit",        "Dr. Maria Lopez",    "3456789012","F32.1","Depression, new onset"),
    ("VIS-018","PAT-010","2024-10-08","Mental Health",     "Dr. Maria Lopez",    "3456789012","F32.1","Psychotherapy session"),
    ("VIS-019","PAT-011","2024-09-11","Sick Visit",        "Dr. James Park",     "2345678901","J45.909","Asthma exacerbation"),
    ("VIS-020","PAT-011","2024-11-25","Follow-up",         "Dr. James Park",     "2345678901","J45.909","Asthma management"),
    ("VIS-021","PAT-012","2024-09-18","Preventive",        "Dr. Sarah Nguyen",   "1234567890","Z00.00","Annual physical 40-64"),
    ("VIS-022","PAT-012","2024-12-10","Sick Visit",        "Dr. Sarah Nguyen",   "1234567890","N39.0","UTI"),
    ("VIS-023","PAT-013","2024-10-01","Preventive",        "Dr. Maria Lopez",    "3456789012","Z00.00","Annual physical 18-39"),
    ("VIS-024","PAT-013","2024-10-01","Immunization",      "Dr. Maria Lopez",    "3456789012","Z23",  "Vaccine administration"),
    ("VIS-025","PAT-014","2024-10-15","Follow-up",         "Dr. James Park",     "2345678901","I10",  "HTN + hyperlipidemia"),
    ("VIS-026","PAT-014","2024-12-01","Sick Visit",        "Dr. James Park",     "2345678901","K21.0","GERD symptoms"),
    ("VIS-027","PAT-015","2024-11-05","Sick Visit",        "Dr. Maria Lopez",    "3456789012","J18.9","Community-acquired pneumonia"),
    ("VIS-028","PAT-015","2024-11-05","Hospital",          "Dr. Maria Lopez",    "3456789012","J18.9","Hospital day 1 — pneumonia"),
    ("VIS-029","PAT-002","2024-12-15","Lab",               "Dr. James Park",     "2345678901","E11.9","Diabetes quarterly labs"),
    ("VIS-030","PAT-006","2024-12-20","Cardiology",        "Dr. Sarah Nguyen",   "1234567890","I25.10","Echo for CAD workup"),
]

# ─────────────────────────────────────────────────────────────────────────────
# 4. VISIT PROCEDURES
# fee_charged, reimbursement chosen to yield realistic payer rates:
#   Insured:   ~80% reimbursement
#   Medicare:  ~75% reimbursement
#   Self-pay:  0% reimbursement (patient pays full)
# ─────────────────────────────────────────────────────────────────────────────
PROCEDURES = [
    # VIS-001 (PAT-001, insured, preventive)
    ("PROC-001","VIS-001","99395",1,185.00,148.00,"Annual preventive visit"),
    ("PROC-002","VIS-001","90739",1, 65.00, 52.00,"Hepatitis B vaccine"),
    ("PROC-003","VIS-001","90471",1, 25.00, 20.00,"Immunization admin"),
    # VIS-002 (PAT-002, insured, diabetes)
    ("PROC-004","VIS-002","99213",1,120.00, 96.00,"Office visit DM follow-up"),
    ("PROC-005","VIS-002","83036",1, 30.00, 24.00,"HbA1c"),
    ("PROC-006","VIS-002","80053",1, 45.00, 36.00,"Comprehensive metabolic panel"),
    ("PROC-007","VIS-002","36415",1, 25.00, 20.00,"Blood draw"),
    # VIS-003 (PAT-002, insured, DM with hyperglycemia)
    ("PROC-008","VIS-003","99214",1,180.00,144.00,"High complexity visit"),
    ("PROC-009","VIS-003","83036",1, 30.00, 24.00,"HbA1c repeat"),
    # VIS-004 (PAT-003, Medicare, hypertension)
    ("PROC-010","VIS-004","99213",1,120.00, 90.00,"HTN follow-up"),
    ("PROC-011","VIS-004","93000",1, 85.00, 63.75,"ECG"),
    ("PROC-012","VIS-004","85025",1, 35.00, 26.25,"CBC"),
    # VIS-005 (PAT-003, Medicare, CAD)
    ("PROC-013","VIS-005","99214",1,180.00,135.00,"Cardio follow-up"),
    ("PROC-014","VIS-005","93000",1, 85.00, 63.75,"ECG"),
    ("PROC-015","VIS-005","80053",1, 45.00, 33.75,"Metabolic panel"),
    # VIS-006 (PAT-004, self-pay, pneumonia)
    ("PROC-016","VIS-006","99203",1,150.00,  0.00,"New patient visit, pneumonia"),
    ("PROC-017","VIS-006","71046",1, 95.00,  0.00,"Chest X-ray"),
    ("PROC-018","VIS-006","J0696",2, 76.00,  0.00,"Ceftriaxone x2"),
    ("PROC-019","VIS-006","85025",1, 35.00,  0.00,"CBC"),
    # VIS-007 (PAT-005, insured, preventive 40-64)
    ("PROC-020","VIS-007","99396",1,210.00,168.00,"Preventive 40-64"),
    ("PROC-021","VIS-007","80053",1, 45.00, 36.00,"Metabolic panel"),
    ("PROC-022","VIS-007","85025",1, 35.00, 28.00,"CBC"),
    ("PROC-023","VIS-007","93000",1, 85.00, 68.00,"ECG"),
    # VIS-008 (PAT-005, insured, hyperlipidemia)
    ("PROC-024","VIS-008","99213",1,120.00, 96.00,"Hyperlipidemia follow-up"),
    ("PROC-025","VIS-008","80053",1, 45.00, 36.00,"Lipid panel"),
    # VIS-009 (PAT-006, Medicare, preventive 65+)
    ("PROC-026","VIS-009","99397",1,240.00,180.00,"Preventive 65+ wellness"),
    ("PROC-027","VIS-009","80053",1, 45.00, 33.75,"Metabolic panel"),
    ("PROC-028","VIS-009","85025",1, 35.00, 26.25,"CBC"),
    ("PROC-029","VIS-009","90686",1, 40.00, 30.00,"Flu vaccine"),
    ("PROC-030","VIS-009","90471",1, 25.00, 18.75,"Immunization admin"),
    # VIS-010 (PAT-006, Medicare, HTN)
    ("PROC-031","VIS-010","99213",1,120.00, 90.00,"HTN review"),
    ("PROC-032","VIS-010","93000",1, 85.00, 63.75,"ECG"),
    # VIS-011 (PAT-007, self-pay, back pain)
    ("PROC-033","VIS-011","99213",1,120.00,  0.00,"Back pain evaluation"),
    ("PROC-034","VIS-011","71046",1, 95.00,  0.00,"Lumbar X-ray"),
    # VIS-012 (PAT-007, self-pay, back pain follow-up)
    ("PROC-035","VIS-012","99213",1,120.00,  0.00,"Back pain follow-up"),
    # VIS-013 (PAT-008, insured, GERD)
    ("PROC-036","VIS-013","99213",1,120.00, 96.00,"GERD follow-up"),
    # VIS-014 (PAT-008, insured, diabetes)
    ("PROC-037","VIS-014","99213",1,120.00, 96.00,"DM check"),
    ("PROC-038","VIS-014","83036",1, 30.00, 24.00,"HbA1c"),
    ("PROC-039","VIS-014","80053",1, 45.00, 36.00,"Metabolic panel"),
    # VIS-015 (PAT-009, Medicare, preventive 65+)
    ("PROC-040","VIS-015","99397",1,240.00,180.00,"Medicare annual wellness"),
    ("PROC-041","VIS-015","80053",1, 45.00, 33.75,"Metabolic panel"),
    ("PROC-042","VIS-015","93000",1, 85.00, 63.75,"ECG"),
    # VIS-016 (PAT-009, Medicare, CAD cardiac)
    ("PROC-043","VIS-016","99214",1,180.00,135.00,"Cardiac follow-up"),
    ("PROC-044","VIS-016","93000",1, 85.00, 63.75,"ECG"),
    # VIS-017 (PAT-010, insured, depression)
    ("PROC-045","VIS-017","99214",1,180.00,144.00,"Depression initial evaluation"),
    # VIS-018 (PAT-010, insured, psychotherapy)
    ("PROC-046","VIS-018","90837",1,150.00,120.00,"Psychotherapy 60 min"),
    # VIS-019 (PAT-011, self-pay, asthma)
    ("PROC-047","VIS-019","99213",1,120.00,  0.00,"Asthma visit"),
    ("PROC-048","VIS-019","94640",1, 55.00,  0.00,"Nebulizer treatment"),
    # VIS-020 (PAT-011, self-pay, asthma follow-up)
    ("PROC-049","VIS-020","99213",1,120.00,  0.00,"Asthma follow-up"),
    # VIS-021 (PAT-012, insured, preventive 40-64)
    ("PROC-050","VIS-021","99396",1,210.00,168.00,"Preventive 40-64"),
    ("PROC-051","VIS-021","80053",1, 45.00, 36.00,"Metabolic panel"),
    ("PROC-052","VIS-021","85025",1, 35.00, 28.00,"CBC"),
    # VIS-022 (PAT-012, insured, UTI)
    ("PROC-053","VIS-022","99213",1,120.00, 96.00,"UTI evaluation"),
    ("PROC-054","VIS-022","J0696",1, 38.00, 30.40,"Ceftriaxone IM"),
    # VIS-023 (PAT-013, insured, preventive 18-39)
    ("PROC-055","VIS-023","99395",1,185.00,148.00,"Annual preventive 18-39"),
    ("PROC-056","VIS-023","80053",1, 45.00, 36.00,"Metabolic panel"),
    # VIS-024 (PAT-013, insured, vaccines)
    ("PROC-057","VIS-024","90686",1, 40.00, 32.00,"Flu vaccine"),
    ("PROC-058","VIS-024","90739",1, 65.00, 52.00,"Hepatitis B vaccine"),
    ("PROC-059","VIS-024","90471",1, 25.00, 20.00,"Immunization admin"),
    # VIS-025 (PAT-014, insured, HTN + hyperlipidemia)
    ("PROC-060","VIS-025","99214",1,180.00,144.00,"HTN/hyperlipidemia"),
    ("PROC-061","VIS-025","80053",1, 45.00, 36.00,"Metabolic + lipid"),
    ("PROC-062","VIS-025","93000",1, 85.00, 68.00,"ECG"),
    # VIS-026 (PAT-014, insured, GERD)
    ("PROC-063","VIS-026","99213",1,120.00, 96.00,"GERD visit"),
    # VIS-027 (PAT-015, insured, pneumonia)
    ("PROC-064","VIS-027","99203",1,150.00,120.00,"Pneumonia new patient"),
    ("PROC-065","VIS-027","71046",1, 95.00, 76.00,"Chest X-ray"),
    ("PROC-066","VIS-027","J0696",2, 76.00, 60.80,"Ceftriaxone x2"),
    ("PROC-067","VIS-027","85025",1, 35.00, 28.00,"CBC"),
    # VIS-028 (PAT-015, insured, hospital)
    ("PROC-068","VIS-028","99232",1,140.00,112.00,"Hospital day subsequent care"),
    # VIS-029 (PAT-002, insured, DM quarterly labs)
    ("PROC-069","VIS-029","83036",1, 30.00, 24.00,"HbA1c quarterly"),
    ("PROC-070","VIS-029","80053",1, 45.00, 36.00,"Metabolic panel"),
    ("PROC-071","VIS-029","36415",1, 25.00, 20.00,"Blood draw"),
    # VIS-030 (PAT-006, Medicare, echo)
    ("PROC-072","VIS-030","93306",1,450.00,337.50,"Echocardiography with Doppler"),
    ("PROC-073","VIS-030","99214",1,180.00,135.00,"Cardiology consultation"),
]

def run(conn):
    cur = conn.cursor()

    # ── Truncate existing data (re-runnable) ──────────────────────────────────
    for t in ["visit_procedures","visits","patients","cpt_codes"]:
        cur.execute(f"DELETE FROM cpt_demo.medical.{t}")
        print(f"  Cleared {t}")

    # ── Insert CPT codes ───────────────────────────────────────────────────────
    cur.executemany(
        "INSERT INTO cpt_demo.medical.cpt_codes VALUES (%s,%s,%s,%s)",
        CPT_CODES,
    )
    print(f"  Inserted {len(CPT_CODES)} CPT codes")

    # ── Insert patients ────────────────────────────────────────────────────────
    for p in PATIENTS:
        cur.execute(
            "INSERT INTO cpt_demo.medical.patients "
            "(patient_id,first_name,last_name,date_of_birth,age,gender,insurance_id) "
            "VALUES (%s,%s,%s,%s,%s,%s,%s)",
            p,
        )
    print(f"  Inserted {len(PATIENTS)} patients")

    # ── Insert visits ──────────────────────────────────────────────────────────
    cur.executemany(
        "INSERT INTO cpt_demo.medical.visits "
        "(visit_id,patient_id,visit_date,visit_type,provider_name,provider_npi,diagnosis_code,notes) "
        "VALUES (%s,%s,%s,%s,%s,%s,%s,%s)",
        VISITS,
    )
    print(f"  Inserted {len(VISITS)} visits")

    # ── Insert procedures ──────────────────────────────────────────────────────
    cur.executemany(
        "INSERT INTO cpt_demo.medical.visit_procedures "
        "(procedure_id,visit_id,cpt_code,units,fee_charged,reimbursement,notes) "
        "VALUES (%s,%s,%s,%s,%s,%s,%s)",
        PROCEDURES,
    )
    print(f"  Inserted {len(PROCEDURES)} procedure lines")

    # ── Rebuild billing_summary view ───────────────────────────────────────────
    cur.execute("""
      CREATE OR REPLACE VIEW cpt_demo.medical.billing_summary AS
      SELECT
        p.patient_id,
        p.first_name,
        p.last_name,
        p.first_name || ' ' || p.last_name   AS patient_name,
        p.gender,
        p.insurance_id,
        CASE WHEN p.insurance_id IS NULL THEN 'Self-Pay' ELSE 'Insured' END AS payer_type,
        v.visit_id,
        v.visit_date,
        MONTH(v.visit_date)                  AS visit_month,
        YEAR(v.visit_date)                   AS visit_year,
        DATE_TRUNC('MONTH', v.visit_date)    AS revenue_month,
        v.visit_type,
        v.provider_name,
        v.diagnosis_code,
        vp.procedure_id,
        vp.cpt_code,
        c.description                        AS procedure_description,
        c.category                           AS procedure_category,
        c.base_fee,
        vp.units,
        vp.fee_charged,
        vp.reimbursement,
        vp.fee_charged - vp.reimbursement    AS patient_balance
      FROM cpt_demo.medical.visit_procedures  vp
      JOIN cpt_demo.medical.visits            v  ON vp.visit_id  = v.visit_id
      JOIN cpt_demo.medical.patients          p  ON v.patient_id = p.patient_id
      JOIN cpt_demo.medical.cpt_codes         c  ON vp.cpt_code  = c.cpt_code
    """)
    print("  Rebuilt billing_summary view")

    # ── Quick verification ─────────────────────────────────────────────────────
    cur.execute("SELECT COUNT(*), SUM(fee_charged), SUM(reimbursement) FROM cpt_demo.medical.billing_summary")
    rows, total_charged, total_reimbursed = cur.fetchone()
    print(f"\n  ✓ billing_summary: {rows} rows | "
          f"Total charged: ${total_charged:,.2f} | "
          f"Total reimbursed: ${total_reimbursed:,.2f}")

    cur.close()

if __name__ == "__main__":
    print("\n  Expanding CPT Medical Billing Dataset…\n")
    conn = get_conn()
    try:
        run(conn)
        print("\n  Done.\n")
    finally:
        conn.close()
