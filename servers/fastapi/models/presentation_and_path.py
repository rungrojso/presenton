from pydantic import BaseModel
import uuid


class PresentationAndPath(BaseModel):
    presentation_id: uuid.UUID
    path: str


class PresentationUrlsMixin(BaseModel):
    """Absolute URLs for direct browser use — populated when
    PUBLIC_BASE_URL is configured; otherwise omitted (None)."""

    download_url: str | None = None
    edit_url: str | None = None


class PresentationPathAndEditPath(PresentationAndPath, PresentationUrlsMixin):
    edit_path: str
