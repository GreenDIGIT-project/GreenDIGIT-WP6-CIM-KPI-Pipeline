from typing import Literal, Optional
from pydantic import BaseModel, Field

SiteType = Literal["cloud", "network", "grid"]

class FactEvent(BaseModel):
    event_start_timestamp: str
    event_end_timestamp: str
    job_finished: bool = True
    CI_g: Optional[int] = None
    CFP_g: Optional[int] = None
    PUE: Optional[float] = None
    site: Optional[str] = None
    energy_wh: Optional[float] = None
    work: Optional[float] = None
    startexectime: str
    stopexectime: str
    status: Optional[str] = "success"
    owner: Optional[str] = None
    execunitid: str
    execunitfinished: bool = True

class CloudDetail(BaseModel):
    wallclocktime_s: Optional[int] = None
    suspendduration_s: Optional[int] = 0
    cpuduration_s: Optional[int] = None
    cpunormalizationfactor: Optional[float] = 1.0
    efficiency: Optional[float] = None
    cloud_type: Optional[str] = "IaaS"
    compute_service: Optional[str] = "EC2"

class NetworkDetail(BaseModel):
    amountofdatatransferred: Optional[int] = None
    networktype: Optional[str] = None
    measurementtype: Optional[str] = None
    destinationexecunitid: Optional[str] = None

class GridDetail(BaseModel):
    wallclocktime_s: Optional[int] = None
    cpunormalizationfactor: Optional[float] = 1.0
    ncores: Optional[int] = None
    normcputime_s: Optional[int] = None
    efficiency: Optional[float] = None
    tdp_w: Optional[int] = None
    totalcputime_s: Optional[int] = None
    scaledcputime_s: Optional[int] = None

class MetricsPayload(BaseModel):
    site_type: SiteType
    site_description: str = Field(..., description="Human readable site description; used to find or create the site")
    fact: FactEvent
    detail: dict

class Sites(BaseModel):
    site_type: Literal["cloud","network","grid","storage","jupyter"]

class Envelope(BaseModel):
    sites: Sites
    fact_site_event: dict
    detail_cloud: Optional[dict] = None
    detail_network: Optional[dict] = None
    detail_grid: Optional[dict] = None
