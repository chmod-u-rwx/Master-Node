from dotenv import load_dotenv
import os

load_dotenv()
CORE_API_URI=os.getenv("CORE_API_URI")
INGRESS_ROUTER_URI=os.getenv("INGRESS_ROUTER_URI")