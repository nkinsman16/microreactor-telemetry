import json
import datetime as dt
from pathlib import Path

raw_path = Path("output/reactor_readings")
raw_path.mkdir(parents=True, exist_ok=True)

event = {
    "reactor_id": "R1",
    "power_mw": 150.5,
    "coolant_temp_c": 300.2,
    "coolant_pressure_mpa": 12.3,
    "coolant_flow_kg_s": 950.0,
    "neutron_flux_nv": 1.2e13,
    "control_rod_position_pct": 45.0,
    "vibration_mm_s": 2.5,
    "core_inlet_temp_c": 280.0,
    "core_outlet_temp_c": 320.0,
    "event_time": dt.datetime.utcnow().isoformat()
}

fname = raw_path / "manual_test_event_R1.json"
with open(fname, "w") as f:
    json.dump(event, f)

print("Wrote", fname)
