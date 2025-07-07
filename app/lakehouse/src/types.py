from typing import List, Literal

from pydantic import BaseModel

Storage = Literal['gcs', 's3', 'hdfs']
FileClass = Literal['structured', 'unstructured']
ProcessingLevel = ['raw', 'processed', 'curated']
FilterOperators = Literal["=",">","<", ">=", "<=", "*", "!="]
CatalogTypes = Literal["files", "collections"]

class CatalogFilter(BaseModel):
    property_name: str
    operator: FilterOperators
    property_value: str | int | float

class CatalogFilterPayload(BaseModel):
    filters: List[CatalogFilter]