import re
from typing import Optional
from pydantic import UUID4, BaseModel, Field, field_validator
import ipaddress

label = r"(?!-)[A-Za-z0-9-]{1,63}(?<!-)"
# Require at least one dot, last part must be TLD of letters only
domain_pattern = re.compile(rf"^{label}(?:\.{label})*\.[A-Za-z]{{2,}}$")

class IngressRouter(BaseModel):
    # ingress_id: UUID4 = Field(...)
    ingress_address: str = Field(...)
    
    @field_validator('ingress_address')
    @classmethod
    def valid_ip_address(cls, v: str):
        v = v.strip()
        
        if not v:
            raise ValueError("ingress_address cannot be empty")

        try:
            ipaddress.ip_address(v)
            return v
        except ValueError:
            ...
        
        if domain_pattern.match(v):
            return v
        
        raise ValueError("ingress_address should contain a valid IP Address or domain name")

class UpdateIngressRouter(BaseModel):
    ingress_address: Optional[str] = None