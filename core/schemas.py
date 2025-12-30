# api/schemas.py
from ninja import Schema
from typing import Optional, List, Dict

class PartyOut(Schema):
    id: int
    country_code: str
    canonical_name: str
    short_name: Optional[str]

class PositionPoint(Schema):
    party_id: int
    party_short_name: str
    source_system: str
    dimension: str
    value: float
    year: int

class TimeSeriesOut(Schema):
    party_id: int
    party_short_name: str
    dimension: str
    source_system: str
    points: List[PositionPoint]

class PopulismPoint(Schema):
    party_id: int
    party_short_name: str
    year: int
    index: float  # indice composito (populismo)

class PopulismSeriesOut(Schema):
    party_id: int
    party_short_name: str
    points: List[PopulismPoint]
    slope_per_10y: float  # pendenza media (delta per 10 anni)
    delta_latest_5y: float

class QualityIssueOut(Schema):
    id: int
    severity: str
    issue_type: str
    details: str
    party_short_name: Optional[str] = None
    region_name: Optional[str] = None
    election_date: Optional[str] = None
    source_system: Optional[str] = None
