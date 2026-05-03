-- ============================================================
-- CPT Coding Case Scenario: Preventive Visit + Hepatitis B Vaccine
-- ============================================================

-- Patients
CREATE TABLE IF NOT EXISTS patients (
    patient_id      VARCHAR(20)     PRIMARY KEY,
    first_name      VARCHAR(50)     NOT NULL,
    last_name       VARCHAR(50)     NOT NULL,
    date_of_birth   DATE            NOT NULL,
    age             INT             NOT NULL,
    gender          VARCHAR(10),
    insurance_id    VARCHAR(30)
);

-- CPT Code reference
CREATE TABLE IF NOT EXISTS cpt_codes (
    cpt_code        VARCHAR(10)     PRIMARY KEY,
    description     VARCHAR(255)    NOT NULL,
    category        VARCHAR(50),
    base_fee        DECIMAL(10,2)
);

-- Clinical visits
CREATE TABLE IF NOT EXISTS visits (
    visit_id        VARCHAR(20)     PRIMARY KEY,
    patient_id      VARCHAR(20)     REFERENCES patients(patient_id),
    visit_date      DATE            NOT NULL,
    visit_type      VARCHAR(50),
    provider_name   VARCHAR(100),
    provider_npi    VARCHAR(20),
    diagnosis_code  VARCHAR(20),
    notes           VARCHAR(500)
);

-- Procedures billed per visit
CREATE TABLE IF NOT EXISTS visit_procedures (
    procedure_id    VARCHAR(20)     PRIMARY KEY,
    visit_id        VARCHAR(20)     REFERENCES visits(visit_id),
    cpt_code        VARCHAR(10)     REFERENCES cpt_codes(cpt_code),
    units           INT             DEFAULT 1,
    fee_charged     DECIMAL(10,2),
    reimbursement   DECIMAL(10,2),
    notes           VARCHAR(255)
);

-- ============================================================
-- Sample Data
-- ============================================================

-- CPT codes
INSERT INTO cpt_codes (cpt_code, description, category, base_fee) VALUES
    ('99395', 'Preventive visit, established patient, age 18-39', 'Evaluation & Management', 185.00),
    ('90739', 'Hepatitis B vaccine, adult dosage, for intramuscular use', 'Vaccine', 65.00),
    ('90471', 'Immunization administration, first injection', 'Administration', 25.00);

-- Patient
INSERT INTO patients (patient_id, first_name, last_name, date_of_birth, age, gender, insurance_id) VALUES
    ('PAT-001', 'James', 'Carter', '1994-03-15', 30, 'Male', 'INS-78432901');

-- Visit
INSERT INTO visits (visit_id, patient_id, visit_date, visit_type, provider_name, provider_npi, diagnosis_code, notes) VALUES
    ('VIS-001', 'PAT-001', '2024-06-10', 'Preventive', 'Dr. Sarah Nguyen', '1234567890', 'Z00.00',
     'Scheduled preventive exam. Patient requested Hepatitis B vaccine. Counseled on vaccine benefits and schedule.');

-- Procedures billed
INSERT INTO visit_procedures (procedure_id, visit_id, cpt_code, units, fee_charged, reimbursement, notes) VALUES
    ('PROC-001', 'VIS-001', '99395', 1, 185.00, 148.00, 'Preventive visit age 18-39'),
    ('PROC-002', 'VIS-001', '90739', 1,  65.00,  52.00, 'Hepatitis B vaccine administered'),
    ('PROC-003', 'VIS-001', '90471', 1,  25.00,  20.00, 'Vaccine administration fee');
