from datetime import datetime
import json
import pytest
from transformations.src.visits_enriched import transform_visits_enriched
from transformations.src.coordinator_impact import transform_coordinator_impact
from transformations.src.patient_silver import transform_patient_silver

patient_schema = (
    "visit_id string, admission_datetime timestamp, age int, gender string, race string, "
    "admission_flag boolean, satisfaction_score int, wait_time int, department_referral string, "
    "coordinator_manager_flag boolean, doctor_id string"
)

doctor_schema = (
    "doctor_id string, doctor_first_name string, doctor_last_name string, department string, "
    "subspecialty string, university string, years_of_experience int"
)


def _patient(visit_id="v1", age=30, wait_time=10, doctor_id="DOC-1"):
    return (
        visit_id, datetime(2024, 1, 15, 8, 30), age, "female", "White", True,
        7, wait_time, "Cardiology", False, doctor_id,
    )


def _doctor(years=10):
    return ("DOC-1", "Jane", "Doe", "Cardiology", "Heart", "MIT", years)


def _bronze(spark, **overrides):   
    """Creates bronze DF with VARIANT column; skip test, when spark does not support VARIANT."""
    
    payload = {
        "patient_id": "P1", "patient_admission_date": "15-01-2024 08:30",
        "patient_first_inital": "ab", "patient_last_name": "SMITH",
        "patient_gender": "Female", "patient_age": "41", "patient_race": "N/A",
        "department_referral": "cardiology", "patient_admission_flag": "Y",
        "patient_satisfaction_score": "8", "patient_waittime": "12",
        "patients_cm": "true", "doctor_id": "DOC-1", "file": "visits.csv", "row": "3",
    }
    payload.update(overrides)
    try:           # dataframe with one row and 2 columns
        df = (
            spark.createDataFrame(
                [(json.dumps(payload), "2024-01-15 09:00:00")],
                ["payload_json", "timestamp_bronze"],
            )
            .selectExpr("parse_json(payload_json) AS payload", "timestamp_bronze")
        )
        df.collect()
    except Exception:
        pytest.skip("parse_json / VARIANT unavailable in this spark version")
    return df


# TEST 1: Visits Enriched 

def test_visits_enriched(spark):
    patients = spark.createDataFrame([
        {
            "visit_id": "v-child", "admission_datetime": datetime(2024, 1, 15, 8, 30, 0),
            "age": 17, "gender": "female", "race": "White", "admission_flag": True,
            "satisfaction_score": 8, "wait_time": 15, "department_referral": None,
            "coordinator_manager_flag": True, "doctor_id": "DOC-1",
        },
        {
            "visit_id": "v-unknown-doc", "admission_datetime": datetime(2024, 1, 15, 22, 0, 0),
            "age": 70, "gender": "male", "race": "Black", "admission_flag": False,
            "satisfaction_score": 4, "wait_time": 50, "department_referral": "Cardiology",
            "coordinator_manager_flag": False, "doctor_id": "DOC-MISSING",
        },
    ])
    doctors = spark.createDataFrame([{
        "doctor_id": "DOC-1", "doctor_first_name": "Jane", "doctor_last_name": "Doe",
        "department": "Cardiology", "subspecialty": "Heart", "university": "MIT",
        "years_of_experience": 16,
    }])

    rows = {r.visit_id: r.asDict() for r in transform_visits_enriched(patients, doctors).collect()}

    child = rows["v-child"]    # child is id of child visit
    assert child["department_referral"] == "No referral"
    assert child["age_group"] == "0-17"
    assert child["wait_bucket_in_mins"] == "00-15"
    assert child["experience_bucket_in_years"] == "16+"
    assert child["coordinator_involved"] == 1
    assert child["doctor_first_name"] == "Jane"

    no_doctor = rows["v-unknown-doc"]    # no_doctor is id of patient with wrong doctor id
    assert no_doctor["doctor_first_name"] is None
    assert no_doctor["age_group"] == "65+"
    assert no_doctor["wait_bucket_in_mins"] == "46+"
    assert no_doctor["experience_bucket_in_years"] is None


@pytest.mark.parametrize("age,expected", [
    (17, "0-17"), (18, "18-39"), (39, "18-39"), (40, "40-64"), (64, "40-64"), (65, "65+"), (None, None),
])
def test_visits_enriched_age_group_boundaries(spark, age, expected):
    patients = spark.createDataFrame([_patient(age=age)], patient_schema)
    doctors = spark.createDataFrame([_doctor()], doctor_schema)
    output = transform_visits_enriched(patients, doctors).collect()[0]
    assert output.age_group == expected


@pytest.mark.parametrize("wait,expected", [
    (0, "00-15"), (15, "00-15"), (16, "16-30"), (30, "16-30"), (31, "31-45"), (45, "31-45"), (46, "46+"),
])
def test_visits_enriched_wait_bucket_boundaries(spark, wait, expected):
    patients = spark.createDataFrame([_patient(wait_time=wait)], patient_schema)
    doctors = spark.createDataFrame([_doctor()], doctor_schema)
    output = transform_visits_enriched(patients, doctors).collect()[0]
    assert output.wait_bucket_in_mins == expected


def test_visits_enriched_does_not_duplicate_visits(spark):
    """Left join test"""
    patients = spark.createDataFrame([_patient("v1"), _patient("v2")], patient_schema)
    doctors = spark.createDataFrame([_doctor()], doctor_schema)
    assert transform_visits_enriched(patients, doctors).count() == 2


# TEST 2: Coordinator Impact

def test_coordinator_impact(spark):
    visits = spark.createDataFrame(
        [("40-64", True, 10, 8, True), ("40-64", False, 30, 5, False), ("40-64", None, 40, 4, False)],
        "age_group string, coordinator_involved boolean, wait_time int, "
        "satisfaction_score int, admission_flag boolean",
    )
    rows = {(r.age_group, r.coordinator): r for r in transform_coordinator_impact(visits).collect()}

    with_cord = rows[("40-64", "with_coordinator")]
    without_cord = rows[("40-64", "without_coordinator")]
    assert with_cord.visits == 1
    assert with_cord.avg_wait_time == 10.0
    assert without_cord.visits == 2  # False and NULL count as 2
    assert without_cord.avg_wait_time == 35.0


def test_coordinator_impact_groups_by_age_group(spark):
    visits = spark.createDataFrame(
        [("0-17", True, 10, 8, True), ("65+", True, 20, 6, False)],
        "age_group string, coordinator_involved boolean, wait_time int, "
        "satisfaction_score int, admission_flag boolean",
    )
    rows = {(r.age_group, r.coordinator) for r in transform_coordinator_impact(visits).collect()}
    assert rows == {("0-17", "with_coordinator"), ("65+", "with_coordinator")}


# TEST 3: Patients Bronze to Silver

def test_patient_silver(spark):
    row = transform_patient_silver(_bronze(spark)).collect()[0]

    assert row.gender == "female"
    assert row.admission_flag is True
    assert row.first_initial == "A"
    assert row.last_name == "Smith"
    assert row.race is None
    assert row.department_referral == "Cardiology"
    assert row.age == 41
    assert row.admission_datetime is not None
    assert len(row.visit_id) == 64


def test_patient_silver_admission_flag_no(spark):
    row = transform_patient_silver(_bronze(spark, patient_admission_flag="N")).collect()[0]
    assert row.admission_flag is False


def test_patient_silver_bad_values_become_null(spark):
    """Wrong date and age can not influence the pipeline"""
    row = transform_patient_silver(
        _bronze(spark, patient_admission_date="not-a-date", patient_age="abc")
    ).collect()[0]
    assert row.admission_datetime is None
    assert row.age is None


def test_patient_silver_visit_id_is_deterministic(spark):
    a = transform_patient_silver(_bronze(spark)).collect()[0].visit_id
    b = transform_patient_silver(_bronze(spark)).collect()[0].visit_id   
    c = transform_patient_silver(_bronze(spark, patient_id="P2")).collect()[0].visit_id

    assert a == b   
    assert a != c   