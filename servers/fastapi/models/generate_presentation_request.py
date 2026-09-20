from typing import List, Literal, Optional
from pydantic import BaseModel, Field

from enums.tone import Tone
from enums.verbosity import Verbosity


class SlideContentInput(BaseModel):
    """Caller-supplied slide payload — skips all LLM calls (render-only mode).

    `layout_index` points into the template's layout list (see
    GET /presentation/layouts/{template}); `content` must satisfy that
    layout's json_schema.
    """

    layout_index: int = Field(
        ..., description="Index into the template's slide layout list"
    )
    content: dict = Field(
        default_factory=dict,
        description="Field values matching the chosen layout's json_schema",
    )
    speaker_note: Optional[str] = Field(
        default=None, description="Speaker notes for this slide"
    )


class GeneratePresentationRequest(BaseModel):
    content: str = Field(..., description="The content for generating the presentation")
    slides_markdown: Optional[List[str]] = Field(
        default=None, description="The markdown for the slides"
    )
    slides_content: Optional[List[SlideContentInput]] = Field(
        default=None,
        description="Ready-made slide contents keyed to layout indices; "
        "when set, no LLM calls are made — the caller is the author",
    )
    layout_payload: Optional[dict] = Field(
        default=None,
        description="Inline template-v2 layout definition (layouts with "
        "components, positions, sizes and elements). When set, the caller's "
        "own geometry is rendered instead of a stored template's",
    )
    instructions: Optional[str] = Field(
        default=None, description="The instruction for generating the presentation"
    )
    tone: Tone = Field(default=Tone.DEFAULT, description="The tone to use for the text")
    verbosity: Verbosity = Field(
        default=Verbosity.STANDARD, description="How verbose the presentation should be"
    )
    web_search: bool = Field(default=False, description="Whether to enable web search")
    n_slides: Optional[int] = Field(
        default=None,
        description="Number of slides to generate. If omitted, model auto-detects slide count.",
    )
    language: Optional[str] = Field(
        default=None,
        description="Language for the presentation. If omitted, model auto-detects language.",
    )
    template: str = Field(
        default="general", description="Template to use for the presentation"
    )
    include_table_of_contents: bool = Field(
        default=False, description="Whether to include a table of contents"
    )
    include_title_slide: bool = Field(
        default=True, description="Whether to include a title slide"
    )
    files: Optional[List[str]] = Field(
        default=None, description="Files to use for the presentation"
    )
    export_as: Literal["pptx", "pdf"] = Field(
        default="pptx", description="Export format"
    )
    trigger_webhook: bool = Field(
        default=False, description="Whether to trigger subscribed webhooks"
    )
