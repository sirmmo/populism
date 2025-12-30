# api/__init__.py
from ninja import NinjaAPI
from .router import router as analysis_router

api = NinjaAPI(title="Political Analysis API")
api.add_router("/analysis", analysis_router)
