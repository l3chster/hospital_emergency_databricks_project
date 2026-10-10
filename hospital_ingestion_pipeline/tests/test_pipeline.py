import builtins
import json
import sys
import types
from datetime import datetime
from importlib import import_module
from pathlib import Path

import pytest
import pyspark
from pyspark.sql import SparkSession

CATALOG = "test_catalog"
PROJEKT = Path(__file__).resolve().parents[1]   # folder hospital_ingestion_pipeline


# ---------- PRZYGOTOWANIE (robi się raz, przed testami) ----------

@pytest.fixture(scope="session")
def spark():
    # 1. Lokalny Spark (w CI nie ma Databricks)
    session = SparkSession.builder.master("local[1]").getOrCreate()

    # 2. Nazwa katalogu, której używają transformacje
    session.conf.set("hospital.ingestion.catalog_name", CATALOG)

    # 3. Pliki z transformations/ używają zmiennej `spark` bez jej tworzenia
    builtins.spark = session

    # 4. Atrapy dekoratorów @dp.view itd. (zamieniają je na "nic nie rób")
    dp = types.ModuleType("pyspark.pipelines")

    def nic_nie_rob(*args, **kwargs):
        if len(args) == 1 and callable(args[0]) and not kwargs:
            return args[0]          # użyte jako @dp.view
        return lambda fn: fn        # użyte jako @dp.view(name="x")

    for nazwa in ["view", "table", "materialized_view", "expect_or_drop", "expect_all"]:
        setattr(dp, nazwa, nic_nie_rob)
    for nazwa in ["create_streaming_table", "create_auto_cdc_flow"]:
        setattr(dp, nazwa, lambda *a, **k: None)

    sys.modules["pyspark.pipelines"] = dp
    pyspark.pipelines = dp

    # 5. Pozwala robić import_module("visits_enriched_gold") itd.
    sys.path.insert(0, str(PROJEKT / "transformations"))

    return session


def podmien_tabele(monkeypatch, czytnik, tabele):
    """Sprawia, że czytnik.table(nazwa) zwraca nasze dane testowe."""
    monkeypatch.setattr(type(czytnik), "table", lambda self, nazwa: tabele[nazwa])


# ---------- TEST 1 ----------

def test_visits_enriched(spark, monkeypatch):
    pacjenci = spark.createDataFrame([
        {   # dziecko, lekarz istnieje, brak skierowania
            "visit_id": "v-child",
            "admission_datetime": datetime(2024, 1, 15, 8, 30, 0),
            "age": 17, "gender": "female", "race": "White",
            "admission_flag": True, "satisfaction_score": 8, "wait_time": 15,
            "department_referral": None, "coordinator_manager_flag": True,
            "doctor_id": "DOC-1",
        },
        {   # senior, lekarza nie ma w tabeli lekarzy
            "visit_id": "v-unknown-doc",
            "admission_datetime": datetime(2024, 1, 15, 22, 0, 0),
            "age": 70, "gender": "male", "race": "Black",
            "admission_flag": False, "satisfaction_score": 4, "wait_time": 50,
            "department_referral": "Cardiology", "coordinator_manager_flag": False,
            "doctor_id": "DOC-MISSING",
        },
    ])
    lekarze = spark.createDataFrame([{
        "doctor_id": "DOC-1", "doctor_first_name": "Jane", "doctor_last_name": "Doe",
        "department": "Cardiology", "subspecialty": "Heart", "university": "MIT",
        "years_of_experience": 16,
    }])

    podmien_tabele(monkeypatch, spark.read, {
        f"{CATALOG}.hospital_silver.patient_streaming_silver": pacjenci,
        f"{CATALOG}.hospital_silver.doctors_silver": lekarze,
    })

    wynik = import_module("visits_enriched_gold").visits_enriched()
    wiersze = {r.visit_id: r.asDict() for r in wynik.collect()}

    dziecko = wiersze["v-child"]
    assert dziecko["department_referral"] == "No referral"
    assert dziecko["age_group"] == "0-17"
    assert dziecko["wait_bucket_in_mins"] == "00-15"
    assert dziecko["experience_bucket_in_years"] == "16+"
    assert dziecko["coordinator_involved"] == 1
    assert dziecko["doctor_first_name"] == "Jane"

    brak = wiersze["v-unknown-doc"]
    assert brak["doctor_first_name"] is None
    assert brak["age_group"] == "65+"
    assert brak["wait_bucket_in_mins"] == "46+"
    assert brak["experience_bucket_in_years"] is None


# ---------- TEST 2 ----------

def test_coordinator_impact(spark, monkeypatch):
    wizyty = spark.createDataFrame(
        [
            ("40-64", True,  10, 8, True),
            ("40-64", False, 30, 5, False),
            ("40-64", None,  40, 4, False),
        ],
        "age_group string, coordinator_involved boolean, wait_time int, "
        "satisfaction_score int, admission_flag boolean",
    )

    podmien_tabele(monkeypatch, spark.read, {
        f"{CATALOG}.hospital_gold.visits_enriched": wizyty,
    })

    wynik = import_module("coordinator_impact").coordinator_impact()
    wiersze = {(r.age_group, r.coordinator): r for r in wynik.collect()}

    z = wiersze[("40-64", "with_coordinator")]
    bez = wiersze[("40-64", "without_coordinator")]

    assert z.visits == 1
    assert z.avg_wait_time == 10.0
    assert bez.visits == 2            # False i NULL liczą się jako "bez"
    assert bez.avg_wait_time == 35.0


# ---------- TEST 3 ----------

def test_patient_silver(spark, monkeypatch):
    dane = {
        "patient_id": "P1",
        "patient_admission_date": "15-01-2024 08:30",
        "patient_first_inital": "ab",
        "patient_last_name": "SMITH",
        "patient_gender": "Female",
        "patient_age": "41",
        "patient_race": "N/A",
        "department_referral": "cardiology",
        "patient_admission_flag": "Y",
        "patient_satisfaction_score": "8",
        "patient_waittime": "12",
        "patients_cm": "true",
        "doctor_id": "DOC-1",
        "file": "visits.csv",
        "row": "3",
    }

    try:
        bronze = spark.createDataFrame(
            [(json.dumps(dane), "2024-01-15 09:00:00")],
            ["payload_json", "timestamp_bronze"],
        ).selectExpr("parse_json(payload_json) AS payload", "timestamp_bronze")
        bronze.collect()
    except Exception:
        pytest.skip("parse_json / VARIANT niedostępne")

    podmien_tabele(monkeypatch, spark.readStream, {"x": None})
    monkeypatch.setattr(type(spark.readStream), "table", lambda self, nazwa: bronze)

    wiersz = import_module("patients_bronze_to_silver").patient_silver_clean().collect()[0]

    assert wiersz.gender == "female"
    assert wiersz.admission_flag is True
    assert wiersz.first_initial == "A"
    assert wiersz.last_name == "Smith"
    assert wiersz.race is None
    assert wiersz.department_referral == "Cardiology"
    assert wiersz.age == 41
    assert wiersz.admission_datetime is not None
    assert len(wiersz.visit_id) == 64