import asyncio
import copy
from datetime import datetime
import json
import logging
import os
import random
import re
import traceback
from typing import Annotated, Any, List, Literal, Optional, Tuple, get_args
import dirtyjson
from fastapi import (
    APIRouter,
    BackgroundTasks,
    Body,
    Depends,
    HTTPException,
    Path,
    Query,
    Request,
)
from fastapi.responses import StreamingResponse
from sqlalchemy import delete
from sqlalchemy.ext.asyncio import AsyncSession
from sqlmodel import select
from constants.presentation import DEFAULT_TEMPLATES, MAX_NUMBER_OF_SLIDES
from enums.webhook_event import WebhookEvent
from models.api_error_model import APIErrorModel
from models.generate_presentation_request import GeneratePresentationRequest
from models.presentation_and_path import PresentationPathAndEditPath
from models.presentation_from_template import EditPresentationRequest
from models.presentation_outline_model import (
    PresentationOutlineModel,
    SlideOutlineModel,
)
from enums.tone import Tone
from enums.verbosity import Verbosity
from models.presentation_structure_model import PresentationStructureModel
from models.presentation_with_slides import (
    PresentationDetailWithSlides,
    PresentationWithSlides,
)
from services.documents_loader import DocumentsLoader
from services.temp_file_service import TEMP_FILE_SERVICE
from services.webhook_service import WebhookService
from services.image_generation_service import ImageGenerationService
from services.mem0_presentation_memory_service import (
    MEM0_PRESENTATION_MEMORY_SERVICE,
)
from utils.dict_utils import deep_update
from utils.export_utils import export_presentation
from utils.llm_calls.generate_presentation_outlines import (
    generate_ppt_outline,
    get_messages as get_outline_messages,
)
from models.sql.slide import SlideModel
from models.sql.presentation_layout_code import PresentationLayoutCodeModel
from models.sse_response import SSECompleteResponse, SSEErrorResponse, SSEResponse

from services.database import get_async_session
from services.database import async_session_maker
from services.concurrent_service import CONCURRENT_SERVICE
from models.sql.presentation import PresentationModel, PresentationVersion
from models.sql.template_v2 import TemplateV2
from models.sql.async_task import AsyncTaskModel
from utils.asset_directory_utils import get_images_directory
from utils.llm_calls.generate_presentation_structure import (
    generate_presentation_structure,
)
from utils.llm_calls.generate_slide_content import (
    get_slide_content_from_type_and_outline,
)
from utils.ppt_utils import (
    select_toc_or_list_slide_layout_index,
)
from utils.outline_utils import (
    get_images_for_slides_from_outline,
    get_no_of_outlines_to_generate_for_n_slides,
    get_no_of_toc_required_for_n_outlines,
    get_presentation_outline_model_with_toc,
    get_presentation_title_from_presentation_outline,
)
from utils.outline_limits import normalize_outline_payload
from utils.process_slides import (
    process_slide_add_placeholder_assets,
    process_slide_and_fetch_assets,
)
from utils.get_layout_by_name import get_layout_by_name
from utils.icon_weights import DEFAULT_ICON_TYPE, extract_icon_type_from_settings
from utils.llm_utils import message_content_to_text
from utils.sse import safe_sse_stream
from utils.get_env import get_public_base_url_env
from utils.simple_auth import (
    SESSION_COOKIE_NAME,
    create_session_token,
    get_session_token_from_request,
)
from utils.web_search import get_selected_web_search_provider, get_web_search_route
from models.presentation_layout import PresentationLayoutModel, SlideLayoutModel
from templates.v2.schema import get_template_schema
import uuid

logger = logging.getLogger(__name__)


PRESENTATION_ROUTER = APIRouter(prefix="/presentation", tags=["Presentation"])
ASYNC_TASK_TYPE_PRESENTATION_GENERATE = "presentation.generate"


def _attach_public_urls(response: PresentationPathAndEditPath):
    """Expand container-relative paths into browser-usable absolute URLs.

    API/MCP callers run outside this container — `path`/`edit_path` alone are
    not resolvable for them. When PUBLIC_BASE_URL is set, also return
    download_url/edit_url so clients can render a working link directly.
    """
    base = get_public_base_url_env()
    if base:
        response.download_url = f"{base}{response.path}"
        response.edit_url = f"{base}{response.edit_path}"
    return response


def _presentation_task_progress_data(
    created_slides: int,
    remaining_slides: int,
) -> dict[str, int]:
    return {
        "created_slides": max(created_slides, 0),
        "remaining_slides": max(remaining_slides, 0),
    }


def _requested_slide_count(request: GeneratePresentationRequest) -> int:
    if request.slides_content:
        return len(request.slides_content)
    if request.slides_markdown:
        return len(request.slides_markdown)
    return request.n_slides or 0


def _extract_custom_template_id(layout_name: Optional[str]) -> Optional[uuid.UUID]:
    if not layout_name or not layout_name.startswith("custom-"):
        return None
    try:
        return uuid.UUID(layout_name.replace("custom-", ""))
    except Exception:
        return None


def _extract_template_v2_id(layout_name: Optional[str]) -> Optional[str]:
    if not isinstance(layout_name, str):
        return None

    layout_name = layout_name.strip()
    if not layout_name:
        return None
    for prefix in ("template-v2-", "template-v2:"):
        if layout_name.startswith(prefix):
            candidate = layout_name[len(prefix) :].strip()
            return candidate or None
    return None


def _extract_requested_template_v2_id(
    template_name: Optional[str],
    *,
    allow_bare: bool = False,
) -> Optional[str]:
    if not isinstance(template_name, str):
        return None

    value = template_name.strip()
    if not value:
        return None
    for prefix in ("template-v2-", "template-v2:", "custom-"):
        if not value.startswith(prefix):
            continue
        candidate = value[len(prefix) :]
        return candidate.strip() or None

    return value if allow_bare else None


def _extract_template_v2_metadata_id(key: str, value: Any) -> Optional[str]:
    if not isinstance(value, str):
        return None

    prefixed = _extract_template_v2_id(value)
    if prefixed:
        return prefixed
    if key in {"template_id", "template_v2_id"}:
        candidate = value.strip()
        return candidate or None
    return None


async def _resolve_requested_template_v2(
    template_name: str,
    sql_session: AsyncSession,
    *,
    allow_bare: bool = False,
) -> Optional[TemplateV2]:
    template_id = _extract_requested_template_v2_id(
        template_name,
        allow_bare=allow_bare,
    )
    if not template_id:
        return None

    template = await sql_session.get(TemplateV2, template_id)
    if template:
        return template

    raise HTTPException(
        status_code=400,
        detail="Template not found. Please use a valid template.",
    )


def _copy_template_v2_layout_payload(
    template: TemplateV2,
) -> dict[str, Any]:
    layout_payload = copy.deepcopy(template.layouts)
    if not isinstance(layout_payload, dict):
        raise HTTPException(
            status_code=400,
            detail="Template v2 layout JSON must be an object",
        )

    icon_type = _template_v2_icon_type(template, layout_payload)
    layout_payload["icon_type"] = icon_type
    layout_payload["icon_weight"] = icon_type
    return layout_payload


def _template_v2_icon_type(
    template: TemplateV2,
    layout_payload: dict[str, Any] | None = None,
) -> str:
    for settings in (template.assets, layout_payload, template.layouts):
        if isinstance(settings, dict):
            icon_type = extract_icon_type_from_settings(settings)
            if icon_type != DEFAULT_ICON_TYPE:
                return icon_type
    return DEFAULT_ICON_TYPE


async def _resolve_generation_layout(
    template_name: str,
    sql_session: AsyncSession,
) -> tuple[dict[str, Any], PresentationLayoutModel, Optional[dict[str, str]], bool]:
    template_v2 = await _resolve_requested_template_v2(template_name, sql_session)
    if template_v2:
        layout_payload = _copy_template_v2_layout_payload(template_v2)
        return (
            layout_payload,
            _build_template_v2_structure_layout(template_v2, layout_payload),
            _extract_template_v2_fonts_from_assets(template_v2.assets),
            True,
        )

    if template_name not in DEFAULT_TEMPLATES:
        raise HTTPException(
            status_code=400,
            detail="Template not found. Please use a valid template.",
        )

    layout_model = await get_layout_by_name(template_name)
    return layout_model.model_dump(mode="json"), layout_model, None, False


def _is_template_v2_slide(slide: SlideModel) -> bool:
    return slide.layout_group.startswith("template-v2") or slide.layout.startswith(
        "template-v2"
    )


def _hydrate_template_v2_slide_ui(
    slide: SlideModel,
    layout_payload: Any = None,
) -> None:
    if not _is_template_v2_slide(slide):
        return

    ui = slide.ui
    if not isinstance(ui, dict):
        ui = _template_v2_slide_ui(layout_payload, slide.layout)
    slide.ui = _apply_template_v2_content_to_ui(ui, slide.content)


def _canonical_template_v2_layout_payload(layout_payload: Any) -> Optional[str]:
    if not _is_template_v2_layout_payload(layout_payload):
        return None

    comparable_payload = copy.deepcopy(layout_payload)
    if isinstance(comparable_payload, dict):
        for metadata_key in ("name", "template_id", "template_v2_id"):
            comparable_payload.pop(metadata_key, None)

    try:
        return json.dumps(
            comparable_payload,
            sort_keys=True,
            separators=(",", ":"),
        )
    except (TypeError, ValueError):
        return None


def _coerce_presentation_font_map(value: Any) -> Optional[dict[str, str]]:
    if not isinstance(value, dict):
        return None

    fonts = {
        name.strip(): url.strip()
        for name, url in value.items()
        if isinstance(name, str)
        and isinstance(url, str)
        and name.strip()
        and url.strip()
    }
    return fonts or None


def _extract_template_v2_fonts_from_assets(assets: Any) -> Optional[dict[str, str]]:
    if not isinstance(assets, dict):
        return None
    return _coerce_presentation_font_map(assets.get("fonts"))


async def _resolve_presentation_template_v2_fonts(
    presentation: PresentationModel,
    slides: List[SlideModel],
    sql_session: AsyncSession,
):
    candidate_template_ids: List[str] = []
    seen: set[str] = set()

    if isinstance(presentation.layout, dict):
        for key in ("name", "template_id", "template_v2_id"):
            value = presentation.layout.get(key)
            template_id = _extract_template_v2_metadata_id(key, value)
            if template_id and template_id not in seen:
                candidate_template_ids.append(template_id)
                seen.add(template_id)

    for slide in slides:
        for value in (slide.layout_group, slide.layout):
            template_id = _extract_template_v2_id(value)
            if template_id and template_id not in seen:
                candidate_template_ids.append(template_id)
                seen.add(template_id)

    for template_id in candidate_template_ids:
        template = await sql_session.get(TemplateV2, template_id)
        if template:
            fonts = _extract_template_v2_fonts_from_assets(template.assets)
            if fonts is not None:
                return fonts

    target_layout_payload = _canonical_template_v2_layout_payload(presentation.layout)
    if not target_layout_payload:
        return None

    try:
        result = await sql_session.execute(
            select(TemplateV2.id, TemplateV2.layouts, TemplateV2.assets)
        )
        for _template_id, layouts, assets in result.all():
            if _canonical_template_v2_layout_payload(layouts) != target_layout_payload:
                continue
            fonts = _extract_template_v2_fonts_from_assets(assets)
            if fonts is not None:
                return fonts
    except Exception:
        logger.exception("[presentation.detail] failed to resolve template v2 fonts")

    return None


async def _resolve_presentation_fonts(
    presentation: PresentationModel,
    slides: List[SlideModel],
    sql_session: AsyncSession,
):
    stored_fonts = _coerce_presentation_font_map(getattr(presentation, "fonts", None))
    if stored_fonts is not None:
        return stored_fonts

    candidate_template_ids: List[uuid.UUID] = []
    seen: set[uuid.UUID] = set()

    layout_name = None
    if isinstance(presentation.layout, dict):
        layout_name = presentation.layout.get("name")
    layout_template_id = _extract_custom_template_id(layout_name)
    if layout_template_id and layout_template_id not in seen:
        candidate_template_ids.append(layout_template_id)
        seen.add(layout_template_id)

    for slide in slides:
        template_id = _extract_custom_template_id(slide.layout_group)
        if template_id and template_id not in seen:
            candidate_template_ids.append(template_id)
            seen.add(template_id)

    for template_id in candidate_template_ids:
        result = await sql_session.execute(
            select(PresentationLayoutCodeModel.fonts).where(
                PresentationLayoutCodeModel.presentation == template_id
            )
        )
        fonts_list = result.scalars().all()
        for fonts in fonts_list:
            if fonts is not None:
                return fonts

    return await _resolve_presentation_template_v2_fonts(
        presentation,
        slides,
        sql_session,
    )


def _presentation_response_data(presentation: PresentationModel) -> dict:
    return presentation.model_dump(exclude={"fonts"})


async def _resolve_presentation_template_v2_payload(
    presentation: PresentationModel,
    slides: List[SlideModel],
    sql_session: AsyncSession,
    payload_field: Literal["components", "merged_components"],
):
    candidate_template_ids: List[str] = []
    seen: set[str] = set()

    if isinstance(presentation.layout, dict):
        for key in ("name", "template_id", "template_v2_id"):
            value = presentation.layout.get(key)
            template_id = _extract_template_v2_metadata_id(key, value)
            if template_id and template_id not in seen:
                candidate_template_ids.append(template_id)
                seen.add(template_id)

    for slide in slides:
        for value in (slide.layout_group, slide.layout):
            template_id = _extract_template_v2_id(value)
            if template_id and template_id not in seen:
                candidate_template_ids.append(template_id)
                seen.add(template_id)

    for template_id in candidate_template_ids:
        template = await sql_session.get(TemplateV2, template_id)
        if template:
            payload = getattr(template, payload_field, None)
            if payload is not None:
                return payload

    target_layout_payload = _canonical_template_v2_layout_payload(presentation.layout)
    if not target_layout_payload:
        return None

    try:
        payload_column = getattr(TemplateV2, payload_field)
        result = await sql_session.execute(
            select(TemplateV2.id, TemplateV2.layouts, payload_column)
        )
        for _template_id, layouts, payload in result.all():
            if payload is None:
                continue
            if _canonical_template_v2_layout_payload(layouts) == target_layout_payload:
                return payload
    except Exception:
        logger.exception(
            "[presentation.detail] failed to resolve template v2 %s",
            payload_field,
        )

    return None


async def _resolve_presentation_merged_components(
    presentation: PresentationModel,
    slides: List[SlideModel],
    sql_session: AsyncSession,
):
    return await _resolve_presentation_template_v2_payload(
        presentation,
        slides,
        sql_session,
        "merged_components",
    )


def _insert_toc_layouts(
    structure: PresentationStructureModel,
    n_toc_slides: int,
    include_title_slide: bool,
    toc_slide_layout_index: int,
):
    if n_toc_slides <= 0 or toc_slide_layout_index == -1:
        return

    insertion_index = 1 if include_title_slide else 0
    for i in range(n_toc_slides):
        structure.slides.insert(insertion_index + i, toc_slide_layout_index)


def _clamp_text_to_schema(
    value: Any,
    schema: dict[str, Any],
    field_path: str,
    adjustments: list[str],
) -> Any:
    """Trim strings to their schema maxLength, at a word boundary when possible.

    Template schemas derive their length caps from the original design, and
    LLM-authored text routinely overshoots them. Trimming (and reporting it)
    renders a usable slide; rejecting the whole deck does not.
    """
    if isinstance(value, dict) and isinstance(schema.get("properties"), dict):
        for key, child_schema in schema["properties"].items():
            if key in value and isinstance(child_schema, dict):
                value[key] = _clamp_text_to_schema(
                    value[key], child_schema, f"{field_path}.{key}", adjustments
                )
        return value

    if isinstance(value, list) and isinstance(schema.get("items"), dict):
        return [
            _clamp_text_to_schema(item, schema["items"], f"{field_path}[{i}]", adjustments)
            for i, item in enumerate(value)
        ]

    if not isinstance(value, str):
        return value

    max_length = schema.get("maxLength")
    if not isinstance(max_length, int) or len(value) <= max_length:
        return value

    trimmed = value[:max_length]
    if " " in trimmed[max_length // 2 :]:
        boundary = trimmed.rfind(" ")
        if boundary > 0:
            trimmed = trimmed[:boundary]
    adjustments.append(
        f"{field_path.lstrip('.')}: trimmed from {len(value)} to "
        f"{len(trimmed)} chars to fit the template"
    )
    return trimmed


def _prepare_slides_content(
    layout_model: PresentationLayoutModel,
    slides_content: list[Any],
) -> list[str]:
    """Clamp length overflows, then validate structure against layout schemas.

    Structural problems (missing required fields, unknown keys, wrong types,
    out-of-range layout_index) still fail fast with a field-by-field list.
    Length violations are auto-fitted and reported via the returned notes.
    """
    from jsonschema import Draft202012Validator

    adjustments: list[str] = []
    problems: list[str] = []
    layout_total = len(layout_model.slides)

    for position, item in enumerate(slides_content):
        layout_index = item.layout_index
        if layout_index < 0 or layout_index >= layout_total:
            problems.append(
                f"slide {position}: layout_index {layout_index} is out of "
                f"range (0..{layout_total - 1})"
            )
            continue

        schema = layout_model.slides[layout_index].json_schema
        _clamp_text_to_schema(item.content, schema, f"slide {position}", adjustments)

        validator = Draft202012Validator(schema)
        for error in sorted(
            validator.iter_errors(item.content),
            key=lambda error: list(error.path),
        ):
            # Length minimums are design hints — short text renders fine.
            if error.validator == "minLength":
                continue
            field_path = ".".join(str(part) for part in error.path) or "(root)"
            problems.append(
                f"slide {position} (layout {layout_index}) field "
                f"'{field_path}': {error.message}"
            )
            if len(problems) >= 20:
                break
        if len(problems) >= 20:
            break

    if problems:
        raise HTTPException(
            status_code=400,
            detail=(
                "slides_content does not match the layout schema:\n- "
                + "\n- ".join(problems)
            ),
        )
    return adjustments


def _layout_count(layout_payload: Any) -> int:
    if isinstance(layout_payload, dict):
        layouts = layout_payload.get("layouts", layout_payload.get("slides"))
        return len(layouts) if isinstance(layouts, list) else 0
    if isinstance(layout_payload, list):
        return len(layout_payload)
    return 0


def _build_template_v2_layout_model(
    layout_payload: dict[str, Any],
    *,
    layout_name: str,
) -> PresentationLayoutModel:
    try:
        template_schema = get_template_schema(layout_payload)
    except ValueError as exc:
        raise HTTPException(
            status_code=400,
            detail=f"Invalid template v2 layout JSON: {exc}",
        ) from exc

    source_layouts = layout_payload.get("layouts")
    if not isinstance(source_layouts, list):
        source_layouts = []

    slides: list[SlideLayoutModel] = []
    for index, schema_layout in enumerate(template_schema["layouts"]):
        if not isinstance(schema_layout, dict):
            continue

        source_layout = (
            source_layouts[index]
            if index < len(source_layouts) and isinstance(source_layouts[index], dict)
            else {}
        )
        layout_id = (
            schema_layout.get("layout_id")
            or source_layout.get("id")
            or f"layout_{index + 1}"
        )
        layout_schema = schema_layout.get("schema")
        if not isinstance(layout_schema, dict):
            layout_schema = {
                "title": str(layout_id),
                "description": source_layout.get("description"),
            }

        slides.append(
            SlideLayoutModel(
                id=str(layout_id),
                name=source_layout.get("name") or layout_schema.get("title"),
                description=source_layout.get("description")
                or layout_schema.get("description"),
                json_schema=layout_schema,
            )
        )

    if not slides:
        raise HTTPException(
            status_code=400,
            detail="Template v2 layout JSON must contain at least one layout",
        )

    return PresentationLayoutModel(
        name=layout_name,
        ordered=False,
        icon_type=extract_icon_type_from_settings(layout_payload),
        slides=slides,
    )


def _build_template_v2_structure_layout(
    template: TemplateV2,
    layout_payload: dict[str, Any],
) -> PresentationLayoutModel:
    return _build_template_v2_layout_model(
        layout_payload,
        layout_name=f"template-v2-{template.id}",
    )


def _is_template_v2_layout_payload(layout_payload: Any) -> bool:
    return (
        isinstance(layout_payload, dict)
        and isinstance(layout_payload.get("layouts"), list)
    )


def _template_v2_slide_ui(
    layout_payload: Any,
    layout_id: str,
) -> Optional[dict[str, Any]]:
    if not _is_template_v2_layout_payload(layout_payload):
        return None

    for layout in layout_payload["layouts"]:
        if isinstance(layout, dict) and str(layout.get("id")) == str(layout_id):
            return copy.deepcopy(layout)
    return None


GENERATED_VALUE_ELEMENT_TYPES = {"text", "image", "text-list", "table", "chart"}
GENERATED_TABLE_TEXT_FONT = {
    "family": "Sniglet",
    "size": 12,
    "color": "#082314",
}
GENERATED_TABLE_HEADER_FONT = {
    **GENERATED_TABLE_TEXT_FONT,
    "bold": True,
}
GENERATED_TABLE_CELL_FILL = {
    "color": "#F8F4E9",
    "opacity": 1,
}
GENERATED_TABLE_CELL_STROKE = {
    "color": "#D8D3C4",
    "opacity": 1,
    "width": 1,
}
TEMPLATE_V2_STRONG_MARKDOWN_DELIMITERS = ("**", "__")
TEMPLATE_V2_EMPHASIS_MARKDOWN_DELIMITERS = ("*", "_")
TEMPLATE_V2_MARKDOWN_DELIMITERS = (
    *TEMPLATE_V2_STRONG_MARKDOWN_DELIMITERS,
    *TEMPLATE_V2_EMPHASIS_MARKDOWN_DELIMITERS,
)


def _template_v2_component_content_keys(components: list[Any]) -> list[str]:
    ids: list[str] = []
    for index, component in enumerate(components):
        component_id = (
            component.get("id")
            if isinstance(component, dict) and isinstance(component.get("id"), str)
            else None
        )
        ids.append(component_id or f"component_{index}")

    counts: dict[str, int] = {}
    for component_id in ids:
        counts[component_id] = counts.get(component_id, 0) + 1

    indexes: dict[str, int] = {}
    used: set[str] = set()
    keys: list[str] = []
    for component_id in ids:
        occurrence_index = indexes.get(component_id, 0)
        indexes[component_id] = occurrence_index + 1
        base = (
            f"{component_id}_{occurrence_index}"
            if counts.get(component_id, 0) > 1
            else component_id
        )

        key = base
        suffix = 1
        while key in used:
            key = f"{base}_{suffix}"
            suffix += 1
        used.add(key)
        keys.append(key)

    return keys


def _apply_template_v2_content_to_ui(
    ui: Optional[dict[str, Any]],
    content: dict[str, Any],
) -> Optional[dict[str, Any]]:
    if not isinstance(ui, dict):
        return ui

    components = ui.get("components")
    if not isinstance(components, list) or not components:
        return ui

    component_keys = _template_v2_component_content_keys(components)
    hydrated_ui = copy.deepcopy(ui)
    hydrated_components = hydrated_ui.get("components")
    if not isinstance(hydrated_components, list):
        return hydrated_ui

    for index, component in enumerate(hydrated_components):
        if not isinstance(component, dict):
            continue

        component_id = component.get("id")
        component_content = content.get(component_keys[index])
        if not isinstance(component_content, dict) and isinstance(component_id, str):
            component_content = content.get(component_id)
        if not isinstance(component_content, dict):
            component_content = {}

        elements = component.get("elements")
        if isinstance(elements, list):
            component["elements"] = _apply_template_v2_content_to_element_list(
                elements,
                component_content,
            )

    return hydrated_ui


def _apply_template_v2_content_to_element(
    element: Any,
    content: Any,
    *,
    direct_value: bool = False,
    preferred_content_keys: list[str] | None = None,
) -> Any:
    if not isinstance(element, dict):
        return element

    content_values = content if isinstance(content, dict) else {}
    element_type = element.get("type")
    name = element.get("name") if isinstance(element.get("name"), str) else None
    has_value = False
    value = None
    if name:
        has_value, value = _template_v2_content_value(
            content_values,
            name,
            preferred_keys=preferred_content_keys,
        )

    if (
        element.get("decorative") is False
        and name
        and has_value
        and element_type in GENERATED_VALUE_ELEMENT_TYPES
    ):
        return _apply_template_v2_content_value(element, value)

    # Repeated flex/grid schemas omit the child-name wrapper when an item is a
    # direct generated value. For example, three image children are emitted as
    # [{"image_prompt": ..., "image_url": ...}, ...], not as
    # [{"gallery_photo": {...}}, ...]. In that case the array item itself is
    # the value for the child element.
    if (
        direct_value
        and not has_value
        and element.get("decorative") is False
        and element_type in GENERATED_VALUE_ELEMENT_TYPES
    ):
        return _apply_template_v2_content_value(element, content)

    nested_content = value if isinstance(value, dict) else content_values
    nested_direct_value = direct_value and not has_value

    if element_type == "container":
        updated = copy.deepcopy(element)
        updated["child"] = _apply_template_v2_content_to_element(
            element.get("child"),
            nested_content,
            direct_value=nested_direct_value,
        )
        return updated

    if element_type in {"flex", "grid", "group"}:
        updated = copy.deepcopy(element)
        children = element.get("children")
        if not isinstance(children, list):
            children = []
        updated["children"] = _apply_template_v2_content_to_children(
            children,
            value,
            nested_content,
            direct_value=nested_direct_value,
        )
        return updated

    return copy.deepcopy(element)


def _template_v2_content_value(
    content: dict[str, Any],
    name: str,
    *,
    preferred_keys: list[str] | None = None,
) -> tuple[bool, Any]:
    candidates: list[str] = []
    for candidate in [
        *(preferred_keys or []),
        *_template_v2_content_name_candidates(name),
    ]:
        if candidate and candidate not in candidates:
            candidates.append(candidate)

    for candidate in candidates:
        if candidate in content:
            return True, content[candidate]
    return False, None


def _template_v2_content_name_candidates(name: str) -> list[str]:
    without_numeric_token = re.sub(r"_\d+(?=_|$)", "", name)
    without_prefix = (
        without_numeric_token.split("_", 1)[1]
        if "_" in without_numeric_token
        else without_numeric_token
    )

    candidates = []
    for candidate in (name, without_numeric_token, without_prefix):
        if candidate and candidate not in candidates:
            candidates.append(candidate)
    return candidates


def _apply_template_v2_content_to_children(
    children: list[Any],
    value: Any,
    content: Any,
    *,
    direct_value: bool = False,
) -> list[Any]:
    if isinstance(value, list) and children:
        return [
            _apply_template_v2_content_to_element(
                children[min(index, len(children) - 1)],
                item,
                direct_value=True,
            )
            for index, item in enumerate(value)
        ]

    return _apply_template_v2_content_to_element_list(
        children,
        content,
        direct_value=direct_value,
    )


def _apply_template_v2_content_to_element_list(
    elements: list[Any],
    content: Any,
    *,
    direct_value: bool = False,
) -> list[Any]:
    content_values = content if isinstance(content, dict) else {}
    name_occurrences: dict[str, int] = {}
    hydrated_elements: list[Any] = []
    for element in elements:
        preferred_keys = _template_v2_repeated_sibling_content_keys(
            element,
            content_values,
            name_occurrences,
        )
        hydrated_elements.append(
            _apply_template_v2_content_to_element(
                element,
                content,
                direct_value=direct_value,
                preferred_content_keys=preferred_keys,
            )
        )
    return hydrated_elements


def _template_v2_repeated_sibling_content_keys(
    element: Any,
    content: dict[str, Any],
    name_occurrences: dict[str, int],
) -> list[str] | None:
    if not isinstance(element, dict):
        return None

    name = element.get("name")
    if not isinstance(name, str) or not name:
        return None

    occurrence_index = name_occurrences.get(name, 0)
    name_occurrences[name] = occurrence_index + 1
    if occurrence_index == 0:
        return None

    suffixed_key = f"{name}_{occurrence_index + 1}"
    return [suffixed_key] if suffixed_key in content else None


def _apply_template_v2_content_value(element: dict[str, Any], value: Any) -> dict[str, Any]:
    element_type = element.get("type")
    if element_type == "text":
        return _apply_template_v2_text_content(element, value)
    if element_type == "image":
        return _apply_template_v2_image_content(element, value)
    if element_type == "text-list":
        return _apply_template_v2_text_list_content(element, value)
    if element_type == "table":
        return _apply_template_v2_table_content(element, value)
    if element_type == "chart":
        return _apply_template_v2_chart_content(element, value)
    return copy.deepcopy(element)


def _read_template_v2_text(value: Any) -> Optional[str]:
    if isinstance(value, str):
        return value
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        return str(value)
    if isinstance(value, dict):
        text = value.get("text")
        if isinstance(text, str):
            return text
        if isinstance(text, (int, float)) and not isinstance(text, bool):
            return str(text)
    return None


def _apply_template_v2_text_content(
    element: dict[str, Any],
    value: Any,
) -> dict[str, Any]:
    text = _read_template_v2_text(value)
    if text is None or text == "":
        return copy.deepcopy(element)

    updated = copy.deepcopy(element)
    first_run = _first_template_v2_text_run(element.get("runs"))
    updated["runs"] = _template_v2_text_runs_from_markdown(
        text,
        first_run,
        fallback_font=element.get("font"),
    )
    updated.pop("text", None)
    return updated


def _apply_template_v2_image_content(
    element: dict[str, Any],
    value: Any,
) -> dict[str, Any]:
    if not isinstance(value, dict):
        return copy.deepcopy(element)

    url = None
    for key in ("image_url", "icon_url", "__image_url__", "__icon_url__", "url"):
        candidate = value.get(key)
        if isinstance(candidate, str) and candidate:
            url = candidate
            break

    if not url:
        return copy.deepcopy(element)

    updated = copy.deepcopy(element)
    updated["data"] = url
    prompt = _template_v2_asset_prompt(value, element.get("is_icon") is True)
    if prompt:
        updated["prompt"] = prompt
    return updated


def _template_v2_asset_prompt(value: Any, is_icon: bool) -> Optional[str]:
    if not isinstance(value, dict):
        return None

    prompt_keys = (
        ("icon_query", "__icon_query__", "query", "prompt")
        if is_icon
        else ("image_prompt", "__image_prompt__", "prompt", "query")
    )
    for key in prompt_keys:
        prompt = value.get(key)
        if isinstance(prompt, str) and prompt.strip():
            return prompt
    return None


def _apply_template_v2_text_list_content(
    element: dict[str, Any],
    value: Any,
) -> dict[str, Any]:
    if not isinstance(value, list):
        return copy.deepcopy(element)

    existing_items = element.get("items")
    if not isinstance(existing_items, list):
        existing_items = []

    items = []
    for index, item in enumerate(value):
        text = _read_template_v2_text(item)
        if text is not None and text != "":
            existing_runs = (
                existing_items[index]
                if index < len(existing_items) and isinstance(existing_items[index], list)
                else None
            )
            first_run = (
                existing_runs[0]
                if isinstance(existing_runs, list)
                and existing_runs
                and isinstance(existing_runs[0], dict)
                else {}
            )
            items.append(
                _template_v2_text_runs_from_markdown(
                    text,
                    first_run,
                    fallback_font=element.get("font"),
                )
            )

    updated = copy.deepcopy(element)
    updated["items"] = items
    return updated


def _apply_template_v2_table_content(
    element: dict[str, Any],
    value: Any,
) -> dict[str, Any]:
    if not isinstance(value, dict):
        return copy.deepcopy(element)

    template_columns = element.get("columns")
    if not isinstance(template_columns, list):
        template_columns = []
    template_rows = [
        row
        for row in element.get("rows", [])
        if isinstance(row, list)
    ]

    generated_columns = [
        _read_template_v2_table_text(item)
        for item in value.get("columns", [])
    ] if isinstance(value.get("columns"), list) else []
    generated_rows = [
        [_read_template_v2_table_text(cell) for cell in row]
        for row in value.get("rows", [])
        if isinstance(row, list)
    ] if isinstance(value.get("rows"), list) else []
    fallback_row = template_rows[-1] if template_rows else template_columns

    updated = copy.deepcopy(element)
    updated["columns"] = (
        _merge_template_v2_table_row_to_length(
            template_columns,
            generated_columns,
            is_header=True,
        )
        if generated_columns
        else copy.deepcopy(template_columns)
    )
    updated["rows"] = (
        [
            _merge_template_v2_table_row_to_length(
                template_rows[index] if index < len(template_rows) else fallback_row,
                row,
                is_header=False,
            )
            for index, row in enumerate(generated_rows)
        ]
        if generated_rows
        else copy.deepcopy(template_rows)
    )
    return updated


def _merge_template_v2_table_row_to_length(
    template_cells: list[Any],
    generated_texts: list[Optional[str]],
    *,
    is_header: bool,
) -> list[Any]:
    fallback_cell = template_cells[-1] if template_cells else None
    return [
        _replace_template_v2_table_cell_text(
            template_cells[index] if index < len(template_cells) else fallback_cell,
            text or "",
            is_header=is_header,
        )
        for index, text in enumerate(generated_texts)
    ]


def _replace_template_v2_table_cell_text(
    cell: Any,
    text: str,
    *,
    is_header: bool,
) -> dict[str, Any]:
    font = GENERATED_TABLE_HEADER_FONT if is_header else GENERATED_TABLE_TEXT_FONT
    if not isinstance(cell, dict):
        return {
            "color": GENERATED_TABLE_CELL_FILL,
            "stroke": GENERATED_TABLE_CELL_STROKE,
            "font": font,
            "runs": _template_v2_text_runs_from_markdown(
                text,
                {"font": font},
            ),
        }

    updated = copy.deepcopy(cell)
    first_run = _first_template_v2_text_run(cell.get("runs"))
    run_font = first_run.get("font") if isinstance(first_run.get("font"), dict) else None
    next_font = run_font or cell.get("font") or font
    updated["color"] = cell.get("color") or cell.get("fill") or GENERATED_TABLE_CELL_FILL
    updated["stroke"] = cell.get("stroke") or GENERATED_TABLE_CELL_STROKE
    updated["font"] = cell.get("font") or next_font
    updated["runs"] = _template_v2_text_runs_from_markdown(
        text,
        first_run,
        fallback_font=next_font,
    )
    updated.pop("text", None)
    updated.pop("fill", None)
    return updated


def _first_template_v2_text_run(runs: Any) -> dict[str, Any]:
    if isinstance(runs, list) and runs and isinstance(runs[0], dict):
        return runs[0]
    return {}


def _template_v2_text_runs_from_markdown(
    text: str,
    first_run: Any,
    *,
    fallback_font: Any = None,
) -> list[dict[str, Any]]:
    base_run = copy.deepcopy(first_run) if isinstance(first_run, dict) else {}
    parsed = _parse_template_v2_markdown_text(text)
    has_markdown_style = any(style for _parsed_text, style in parsed)
    base_run = _template_v2_base_run_for_markdown(
        base_run,
        fallback_font,
        strip_inline_emphasis=has_markdown_style,
    )

    text_runs: list[dict[str, Any]] = []
    for parsed_text, style in parsed:
        run = copy.deepcopy(base_run)
        run["text"] = parsed_text
        if style:
            font = run.get("font")
            run["font"] = {
                **(copy.deepcopy(font) if isinstance(font, dict) else {}),
                **style,
            }
        _append_template_v2_text_run(text_runs, run)

    if text_runs:
        return text_runs
    return [{**base_run, "text": " "}]


def _template_v2_base_run_for_markdown(
    base_run: dict[str, Any],
    fallback_font: Any,
    *,
    strip_inline_emphasis: bool,
) -> dict[str, Any]:
    font = base_run.get("font")
    if isinstance(fallback_font, dict):
        merged_font = {
            **copy.deepcopy(fallback_font),
            **(copy.deepcopy(font) if isinstance(font, dict) else {}),
        }
        base_run["font"] = merged_font
    elif isinstance(font, dict):
        base_run["font"] = copy.deepcopy(font)

    if strip_inline_emphasis and isinstance(base_run.get("font"), dict):
        base_run["font"].pop("bold", None)
        base_run["font"].pop("italic", None)

    return base_run


def _parse_template_v2_markdown_text(
    text: str,
) -> list[tuple[str, dict[str, bool]]]:
    parsed: list[tuple[str, dict[str, bool]]] = []
    index = 0

    while index < len(text):
        strong_delimiter = _template_v2_read_markdown_delimiter(
            text,
            index,
            TEMPLATE_V2_STRONG_MARKDOWN_DELIMITERS,
        )
        if strong_delimiter:
            close = text.find(strong_delimiter, index + len(strong_delimiter))
            if close > index + len(strong_delimiter):
                parsed.append(
                    (
                        text[index + len(strong_delimiter) : close],
                        {"bold": True},
                    )
                )
                index = close + len(strong_delimiter)
                continue

        emphasis_delimiter = _template_v2_read_markdown_delimiter(
            text,
            index,
            TEMPLATE_V2_EMPHASIS_MARKDOWN_DELIMITERS,
        )
        if emphasis_delimiter:
            close = text.find(emphasis_delimiter, index + len(emphasis_delimiter))
            if close > index + len(emphasis_delimiter):
                parsed.append(
                    (
                        text[index + len(emphasis_delimiter) : close],
                        {"italic": True},
                    )
                )
                index = close + len(emphasis_delimiter)
                continue

        next_index = _template_v2_next_markdown_delimiter_index(text, index + 1)
        parsed.append(
            (
                text[index : len(text) if next_index == -1 else next_index],
                {},
            )
        )
        index = len(text) if next_index == -1 else next_index

    return parsed


def _template_v2_read_markdown_delimiter(
    text: str,
    index: int,
    delimiters: tuple[str, ...],
) -> Optional[str]:
    for delimiter in delimiters:
        if text.startswith(delimiter, index):
            return delimiter
    return None


def _template_v2_next_markdown_delimiter_index(text: str, start: int) -> int:
    indexes = [
        index
        for index in (
            text.find(delimiter, start)
            for delimiter in TEMPLATE_V2_MARKDOWN_DELIMITERS
        )
        if index != -1
    ]
    return min(indexes) if indexes else -1


def _append_template_v2_text_run(
    text_runs: list[dict[str, Any]],
    run: dict[str, Any],
) -> None:
    text = run.get("text")
    if not isinstance(text, str) or text == "":
        return

    previous = text_runs[-1] if text_runs else None
    if isinstance(previous, dict):
        previous_style = {key: value for key, value in previous.items() if key != "text"}
        next_style = {key: value for key, value in run.items() if key != "text"}
        if previous_style == next_style and isinstance(previous.get("text"), str):
            previous["text"] += text
            return

    text_runs.append(run)


def _read_template_v2_table_text(value: Any) -> Optional[str]:
    primitive_text = _read_template_v2_primitive_table_text(value)
    if primitive_text is not None:
        return primitive_text[:80]

    if not isinstance(value, dict):
        return None

    runs = value.get("runs")
    if isinstance(runs, list):
        run_text = "".join(
            run.get("text", "")
            for run in runs
            if isinstance(run, dict) and isinstance(run.get("text"), str)
        )
        if run_text:
            return run_text[:80]

    for key in ("text", "value"):
        text = _read_template_v2_primitive_table_text(value.get(key))
        if text is not None:
            return text[:80]

    return None


def _read_template_v2_primitive_table_text(value: Any) -> Optional[str]:
    if isinstance(value, str):
        return value
    if isinstance(value, bool):
        return str(value).lower()
    if isinstance(value, (int, float)):
        return str(value)
    return None


def _read_template_v2_data_labels(value: Any) -> Optional[str]:
    if value is True:
        return "top"
    if value is False or value is None:
        return None
    if isinstance(value, str):
        normalized = value.strip().lower()
        if normalized in {"base", "mid", "top", "outside"}:
            return normalized
    return None


def _apply_template_v2_chart_content(
    element: dict[str, Any],
    value: Any,
) -> dict[str, Any]:
    updated = copy.deepcopy(element)
    updated.pop("data_labels_color", None)
    updated.pop("grid", None)
    if not isinstance(value, dict):
        return updated

    chart_type = value.get("chartType", value.get("chart_type"))
    if chart_type in {
        "area",
        "bar",
        "bubble",
        "donut",
        "horizontal_bar",
        "horizontal_stacked_bar",
        "line",
        "pie",
        "polar_area",
        "radar",
        "scatter",
        "stacked_bar",
    }:
        updated["chart_type"] = chart_type
    if isinstance(value.get("title"), str):
        updated["title"] = value["title"]
    if isinstance(value.get("categories"), list) and value["categories"]:
        updated["categories"] = value["categories"]
    if isinstance(value.get("series"), list) and value["series"]:
        updated["series"] = value["series"]
    colors = value.get("colors")
    if isinstance(colors, list) and colors:
        updated["colors"] = colors
    for source_key, target_key in (
        ("axisColor", "axis_color"),
        ("axis_color", "axis_color"),
        ("gridColor", "grid_color"),
        ("grid_color", "grid_color"),
        ("xAxisTitle", "x_axis_title"),
        ("x_axis_title", "x_axis_title"),
        ("yAxisTitle", "y_axis_title"),
        ("y_axis_title", "y_axis_title"),
        ("source", "source"),
    ):
        if isinstance(value.get(source_key), str):
            updated[target_key] = value[source_key]
    for source_key, target_key in (
        ("xAxis", "x_axis"),
        ("x_axis", "x_axis"),
        ("yAxis", "y_axis"),
        ("y_axis", "y_axis"),
        ("xAxisGrid", "x_axis_grid"),
        ("x_axis_grid", "x_axis_grid"),
        ("yAxisGrid", "y_axis_grid"),
        ("y_axis_grid", "y_axis_grid"),
    ):
        if isinstance(value.get(source_key), bool):
            updated[target_key] = value[source_key]
    for source_key in ("dataLabels", "data_labels"):
        if source_key in value:
            updated["data_labels"] = _read_template_v2_data_labels(value.get(source_key))
    return updated


def _get_presentation_stream_layout(
    presentation: PresentationModel,
) -> PresentationLayoutModel:
    if _is_template_v2_layout_payload(presentation.layout):
        layout_name = str(presentation.layout.get("name") or "template-v2")
        return _build_template_v2_layout_model(
            presentation.layout,
            layout_name=layout_name,
        )

    return presentation.get_layout()


async def _resolve_prepare_layout(
    layout: PresentationLayoutModel | str,
    sql_session: AsyncSession,
) -> tuple[dict[str, Any], PresentationLayoutModel, Optional[dict[str, str]]]:
    if isinstance(layout, PresentationLayoutModel):
        return layout.model_dump(mode="json"), layout, None

    template_id = layout.strip()
    if not template_id:
        raise HTTPException(
            status_code=400,
            detail="Template v2 layout id is required",
        )

    template = await sql_session.get(TemplateV2, template_id)
    if not template:
        raise HTTPException(status_code=404, detail="Template v2 layout not found")

    layout_payload = _copy_template_v2_layout_payload(template)

    structure_layout = _build_template_v2_structure_layout(template, layout_payload)
    return (
        layout_payload,
        structure_layout,
        _extract_template_v2_fonts_from_assets(template.assets),
    )


def _build_export_cookie_header(request: Request) -> Optional[str]:
    cookie_header = (request.headers.get("cookie") or "").strip()
    if cookie_header:
        return cookie_header

    session_token = get_session_token_from_request(request)
    if session_token:
        return f"{SESSION_COOKIE_NAME}={session_token}"

    username = getattr(request.state, "auth_username", None)
    if isinstance(username, str) and username.strip():
        try:
            session_token = create_session_token(username.strip())
            return f"{SESSION_COOKIE_NAME}={session_token}"
        except Exception:
            logger.exception(
                "[presentation.generate] failed to create export session token"
            )

    return None


@PRESENTATION_ROUTER.get("/all", response_model=List[PresentationWithSlides])
async def get_all_presentations(
    version: Annotated[
        Optional[PresentationVersion],
        Query(description="Only include presentations matching this version."),
    ] = None,
    include_slides: Annotated[
        bool,
        Query(
            description=(
                "Include the first slide and resolved fonts for dashboard previews. "
                "Disable this for metadata-only presentation lists."
            )
        ),
    ] = True,
    sql_session: AsyncSession = Depends(get_async_session),
):
    if include_slides:
        query = select(PresentationModel, SlideModel).join(
            SlideModel,
            (SlideModel.presentation == PresentationModel.id) & (SlideModel.index == 0),
        )
    else:
        query = select(PresentationModel)

    if version is not None:
        query = query.where(PresentationModel.version == version)
    query = query.order_by(PresentationModel.created_at.desc())

    results = await sql_session.execute(query)
    if not include_slides:
        return [
            PresentationWithSlides(
                **_presentation_response_data(presentation),
                slides=[],
                fonts=None,
            )
            for presentation in results.scalars().all()
        ]

    rows = results.all()
    presentations_with_slides = []
    for presentation, first_slide in rows:
        slides = [first_slide]
        fonts = await _resolve_presentation_fonts(presentation, slides, sql_session)
        presentations_with_slides.append(
            PresentationWithSlides(
                **_presentation_response_data(presentation),
                slides=slides,
                fonts=fonts,
            )
        )
    return presentations_with_slides


@PRESENTATION_ROUTER.get("/layouts/{template_arg}")
async def get_template_slide_layouts(
    template_arg: str,
    sql_session: AsyncSession = Depends(get_async_session),
):
    """Resolved slide layouts for a template arg (`general` or `custom-<uuid>`).

    Each entry carries `index` (the value `slides_content[].layout_index`
    expects), the layout `id`, and its `json_schema` — everything a caller
    needs to author slide contents in render-only mode without any LLM calls.
    """
    _, layout_model, _, _ = await _resolve_generation_layout(
        template_arg, sql_session
    )
    return [
        {
            "index": index,
            "id": slide.id,
            "name": slide.name,
            "description": slide.description,
            "json_schema": slide.json_schema,
        }
        for index, slide in enumerate(layout_model.slides)
    ]


@PRESENTATION_ROUTER.get("/element-schema")
async def get_element_schema():
    """JSON schema for every slide element type.

    Covers the full element vocabulary (text, image, chart, table, flex, …)
    with fields and enums — what a caller needs to author or patch elements
    inside a layout_payload or a chat-tool element update.
    """
    from templates.v2.models.elements import (
        Chart,
        Container,
        Ellipse,
        Flex,
        Grid,
        Group,
        Image,
        Infographic,
        Line,
        Rectangle,
        Table,
        Text,
        TextList,
    )

    models = [
        Text, Container, Image, TextList, Table, Rectangle, Ellipse, Line,
        Chart, Infographic, Flex, Grid, Group,
    ]
    elements = []
    for model in models:
        type_args = get_args(model.model_fields["type"].annotation)
        elements.append(
            {
                "type": type_args[0] if type_args else model.__name__.lower(),
                "schema": model.model_json_schema(),
            }
        )
    return {"elements": elements}


@PRESENTATION_ROUTER.get("/layout-payload/{template_arg}")
async def get_template_layout_payload(
    template_arg: str,
    sql_session: AsyncSession = Depends(get_async_session),
):
    """Raw layout definition for a template arg — the caller's starting point
    for pixel-level control.

    Returns the full template-v2 payload (layouts → components → position,
    size, elements). Edit it and pass it back as `layout_payload` on
    generate to render your own geometry instead of the stored template's.
    """
    template_v2 = await _resolve_requested_template_v2(template_arg, sql_session)
    if template_v2:
        return _copy_template_v2_layout_payload(template_v2)

    if template_arg not in DEFAULT_TEMPLATES:
        raise HTTPException(
            status_code=400,
            detail="Template not found. Please use a valid template.",
        )

    layout_model = await get_layout_by_name(template_arg)
    return layout_model.model_dump(mode="json")


@PRESENTATION_ROUTER.get("/{id}", response_model=PresentationDetailWithSlides)
async def get_presentation(
    id: uuid.UUID,
    request: Request,
    sql_session: AsyncSession = Depends(get_async_session),
):
    presentation = await sql_session.get(PresentationModel, id)
    if not presentation:
        raise HTTPException(404, "Presentation not found")
    slides_result = await sql_session.scalars(
        select(SlideModel)
        .where(SlideModel.presentation == id)
        .order_by(SlideModel.index)
    )
    slides = list(slides_result)
    fonts = await _resolve_presentation_fonts(presentation, slides, sql_session)
    merged_components = await _resolve_presentation_merged_components(
        presentation,
        slides,
        sql_session,
    )
    return PresentationDetailWithSlides(
        **_presentation_response_data(presentation),
        slides=slides,
        fonts=fonts,
        merged_components=merged_components,
    )


@PRESENTATION_ROUTER.delete("/{id}", status_code=204)
async def delete_presentation(
    id: uuid.UUID, sql_session: AsyncSession = Depends(get_async_session)
):
    presentation = await sql_session.get(PresentationModel, id)
    if not presentation:
        raise HTTPException(404, "Presentation not found")

    await sql_session.delete(presentation)
    await sql_session.commit()


@PRESENTATION_ROUTER.post("/{id}/duplicate", response_model=PresentationWithSlides)
async def duplicate_presentation(
    id: uuid.UUID, sql_session: AsyncSession = Depends(get_async_session)
):
    presentation = await sql_session.get(PresentationModel, id)
    if not presentation:
        raise HTTPException(404, "Presentation not found")

    slides = list(
        await sql_session.scalars(
            select(SlideModel)
            .where(SlideModel.presentation == id)
            .order_by(SlideModel.index)
        )
    )
    new_presentation = presentation.get_new_presentation()
    if new_presentation.title:
        new_presentation.title = f"{new_presentation.title} (Copy)"
    new_slides = [slide.get_new_slide(new_presentation.id) for slide in slides]

    sql_session.add(new_presentation)
    sql_session.add_all(new_slides)
    await sql_session.commit()
    await sql_session.refresh(new_presentation)

    return PresentationWithSlides(
        **_presentation_response_data(new_presentation),
        slides=new_slides,
        fonts=await _resolve_presentation_fonts(
            new_presentation,
            new_slides,
            sql_session,
        ),
    )


@PRESENTATION_ROUTER.post("/create", response_model=PresentationModel)
async def create_presentation(
    content: Annotated[str, Body()],
    n_slides: Annotated[Optional[int], Body()] = None,
    language: Annotated[Optional[str], Body()] = None,
    file_paths: Annotated[Optional[List[str]], Body()] = None,
    tone: Annotated[Tone, Body()] = Tone.DEFAULT,
    verbosity: Annotated[Verbosity, Body()] = Verbosity.STANDARD,
    instructions: Annotated[Optional[str], Body()] = None,
    include_table_of_contents: Annotated[bool, Body()] = False,
    include_title_slide: Annotated[bool, Body()] = True,
    web_search: Annotated[bool, Body()] = False,
    sql_session: AsyncSession = Depends(get_async_session),
):

    if n_slides is not None and n_slides < 1:
        raise HTTPException(
            status_code=400,
            detail="Number of slides must be greater than 0",
        )

    if n_slides is not None and n_slides > MAX_NUMBER_OF_SLIDES:
        raise HTTPException(
            status_code=400,
            detail=f"Number of slides cannot be greater than {MAX_NUMBER_OF_SLIDES}",
        )

    if include_table_of_contents and n_slides is not None and n_slides < 3:
        raise HTTPException(
            status_code=400,
            detail="Number of slides cannot be less than 3 if table of contents is included",
    )

    presentation_id = uuid.uuid4()
    language_to_store = (language or "").strip()
    validated_file_paths = (
        TEMP_FILE_SERVICE.resolve_existing_temp_paths(file_paths)
        if file_paths
        else None
    )
    # DB schema stores an int; 0 is used as internal marker for auto slide count.
    n_slides_to_store = n_slides if n_slides is not None else 0

    presentation = PresentationModel(
        id=presentation_id,
        version=PresentationVersion.V2_STANDARD,
        content=content,
        n_slides=n_slides_to_store,
        language=language_to_store,
        file_paths=validated_file_paths,
        tone=tone.value,
        verbosity=verbosity.value,
        instructions=instructions,
        include_table_of_contents=include_table_of_contents,
        include_title_slide=include_title_slide,
        web_search=web_search,
    )

    sql_session.add(presentation)
    await sql_session.commit()

    search_route, actual_search_provider = get_web_search_route()
    logger.info(
        "Created presentation: id=%s web_search_enabled=%s selected_web_search_provider=%s "
        "web_search_route=%s actual_web_search_provider=%s",
        presentation_id,
        web_search,
        get_selected_web_search_provider().value,
        search_route,
        (
            actual_search_provider.value
            if actual_search_provider
            else ("model-native" if search_route == "native" else "none")
        ),
    )

    return presentation


@PRESENTATION_ROUTER.post("/prepare", response_model=PresentationModel)
async def prepare_presentation(
    presentation_id: Annotated[uuid.UUID, Body()],
    outlines: Annotated[List[SlideOutlineModel], Body()],
    layout: Annotated[PresentationLayoutModel | str, Body()],
    title: Annotated[Optional[str], Body()] = None,
    sql_session: AsyncSession = Depends(get_async_session),
):
    if not outlines:
        raise HTTPException(status_code=400, detail="Outlines are required")
    if len(outlines) > MAX_NUMBER_OF_SLIDES:
        raise HTTPException(
            status_code=400,
            detail=f"Number of outlines cannot be greater than {MAX_NUMBER_OF_SLIDES}",
        )

    presentation = await sql_session.get(PresentationModel, presentation_id)
    if not presentation:
        raise HTTPException(status_code=404, detail="Presentation not found")

    presentation_outline_model = PresentationOutlineModel(slides=outlines)

    layout_payload, structure_layout, template_fonts = await _resolve_prepare_layout(
        layout, sql_session
    )
    total_slide_layouts = _layout_count(layout_payload)
    if total_slide_layouts < 1:
        raise HTTPException(
            status_code=400,
            detail="Layout must contain at least one slide layout",
        )
    total_outlines = len(outlines)

    if structure_layout.ordered:
        presentation_structure = structure_layout.to_presentation_structure()
    else:
        presentation_structure: PresentationStructureModel = (
            await generate_presentation_structure(
                presentation_outline=presentation_outline_model,
                presentation_layout=structure_layout,
                instructions=presentation.instructions,
            )
        )

    presentation_structure.slides = presentation_structure.slides[: len(outlines)]
    for index in range(total_outlines):
        random_slide_index = random.randint(0, total_slide_layouts - 1)
        if index >= total_outlines:
            presentation_structure.slides.append(random_slide_index)
            continue
        if presentation_structure.slides[index] >= total_slide_layouts:
            presentation_structure.slides[index] = random_slide_index

    if presentation.include_table_of_contents:
        n_toc_slides = get_no_of_toc_required_for_n_outlines(
            n_outlines=total_outlines,
            title_slide=presentation.include_title_slide,
            target_total_slides=(presentation.n_slides if presentation.n_slides > 0 else None),
        )
        toc_slide_layout_index = select_toc_or_list_slide_layout_index(structure_layout)
        _insert_toc_layouts(
            presentation_structure,
            n_toc_slides,
            presentation.include_title_slide,
            toc_slide_layout_index,
        )
        if toc_slide_layout_index != -1 and n_toc_slides > 0:
            presentation_outline_model = get_presentation_outline_model_with_toc(
                outline=presentation_outline_model,
                n_toc_slides=n_toc_slides,
                title_slide=presentation.include_title_slide,
            )

    sql_session.add(presentation)
    presentation.outlines = presentation_outline_model.model_dump(mode="json")
    presentation.title = title or presentation.title
    # Final slide generation should follow the reviewed outline text. The
    # original upload language can be stale after outline-page chat edits such
    # as "convert these to Chinese".
    presentation.language = ""
    presentation.layout = layout_payload
    presentation.fonts = template_fonts
    presentation.set_structure(presentation_structure)
    await sql_session.commit()

    await MEM0_PRESENTATION_MEMORY_SERVICE.store_generated_outlines(
        presentation.id,
        presentation.outlines,
    )

    return presentation


@PRESENTATION_ROUTER.get("/stream/{id}", response_model=PresentationDetailWithSlides)
async def stream_presentation(
    id: uuid.UUID, sql_session: AsyncSession = Depends(get_async_session)
):
    presentation = await sql_session.get(PresentationModel, id)
    if not presentation:
        raise HTTPException(status_code=404, detail="Presentation not found")
    if not presentation.structure:
        raise HTTPException(
            status_code=400,
            detail="Presentation not prepared for stream",
        )
    if not presentation.outlines:
        raise HTTPException(
            status_code=400,
            detail="Outlines can not be empty",
        )

    try:
        structure = presentation.get_structure()
        layout = _get_presentation_stream_layout(presentation)
        outline = presentation.get_presentation_outline()
    except Exception as exc:
        raise HTTPException(
            status_code=400,
            detail="Presentation has invalid generated data",
        ) from exc

    if not layout.slides:
        raise HTTPException(status_code=400, detail="Presentation layout has no slides")
    if len(structure.slides) > len(outline.slides):
        raise HTTPException(
            status_code=400,
            detail="Presentation structure has more slides than outlines",
        )
    invalid_layout_index = next(
        (
            slide_layout_index
            for slide_layout_index in structure.slides
            if slide_layout_index < 0 or slide_layout_index >= len(layout.slides)
        ),
        None,
    )
    if invalid_layout_index is not None:
        raise HTTPException(
            status_code=400,
            detail="Presentation structure contains an invalid slide layout",
        )

    image_generation_service = ImageGenerationService(get_images_directory())

    async def inner():
        icon_weight = layout.icon_weight
        image_urls_for_slides = get_images_for_slides_from_outline(outline.slides)

        async_assets_generation_tasks: List[asyncio.Task] = []
        asset_events: asyncio.Queue = asyncio.Queue()
        asset_warnings_by_slide: dict[int, list[dict]] = {}

        async def notify_slide_assets_ready(slide_index: int, asset_task: asyncio.Task):
            try:
                await asset_task
            except Exception:
                logger.exception(
                    "Slide asset generation failed: presentation_id=%s slide_index=%s",
                    id,
                    slide_index,
                )
                asset_warnings_by_slide.setdefault(slide_index, []).append(
                    {
                        "type": "asset_generation_failed",
                        "message": "Some slide assets could not be generated.",
                    }
                )
            finally:
                await asset_events.put(slide_index)

        slides: List[SlideModel] = []
        yield SSEResponse(
            event="response",
            data=json.dumps({"type": "chunk", "chunk": '{ "slides": [ '}),
        ).to_string()
        yielded_slide_asset_sse_count = 0

        for i, slide_layout_index in enumerate(structure.slides):
            slide_layout = layout.slides[slide_layout_index]

            try:
                slide_content = await get_slide_content_from_type_and_outline(
                    slide_layout,
                    outline.slides[i],
                    presentation.language,
                    presentation.tone,
                    presentation.verbosity,
                    presentation.instructions,
                )
            except HTTPException as e:
                yield SSEErrorResponse(detail=e.detail).to_string()
                return

            slide = SlideModel(
                presentation=id,
                layout_group=layout.name,
                layout=slide_layout.id,
                index=i,
                speaker_note=slide_content.get("__speaker_note__", ""),
                content=slide_content,
                ui=_template_v2_slide_ui(presentation.layout, slide_layout.id),
            )
            slides.append(slide)

            # This will mutate slide and add placeholder assets
            process_slide_add_placeholder_assets(slide)
            slide.ui = _apply_template_v2_content_to_ui(slide.ui, slide.content)

            # This will mutate slide - start task immediately so it runs in parallel with next slide LLM generation
            asset_warnings_by_slide[i] = []
            asset_task = asyncio.create_task(
                process_slide_and_fetch_assets(
                    image_generation_service,
                    slide,
                    outline_image_urls=(
                        image_urls_for_slides[i]
                        if i < len(image_urls_for_slides)
                        else None
                    ),
                    icon_weight=icon_weight,
                    allow_image_fallback=True,
                    image_warnings=asset_warnings_by_slide[i],
                )
            )
            async_assets_generation_tasks.append(asset_task)
            asyncio.create_task(notify_slide_assets_ready(i, asset_task))

            yield SSEResponse(
                event="response",
                data=json.dumps({"type": "chunk", "chunk": slide.model_dump_json()}),
            ).to_string()

            while True:
                try:
                    done_idx = asset_events.get_nowait()
                except asyncio.QueueEmpty:
                    break
                slides[done_idx].ui = _apply_template_v2_content_to_ui(
                    slides[done_idx].ui,
                    slides[done_idx].content,
                )
                yielded_slide_asset_sse_count += 1
                yield SSEResponse(
                    event="response",
                    data=json.dumps(
                        {
                            "type": "slide_assets",
                            "slide_index": done_idx,
                            "slide": slides[done_idx].model_dump(mode="json"),
                            "warnings": asset_warnings_by_slide.get(done_idx, []),
                        }
                    ),
                ).to_string()

        yield SSEResponse(
            event="response",
            data=json.dumps({"type": "chunk", "chunk": " ] }"}),
        ).to_string()

        while yielded_slide_asset_sse_count < len(slides):
            done_idx = await asset_events.get()
            slides[done_idx].ui = _apply_template_v2_content_to_ui(
                slides[done_idx].ui,
                slides[done_idx].content,
            )
            yielded_slide_asset_sse_count += 1
            yield SSEResponse(
                event="response",
                data=json.dumps(
                    {
                        "type": "slide_assets",
                        "slide_index": done_idx,
                        "slide": slides[done_idx].model_dump(mode="json"),
                        "warnings": asset_warnings_by_slide.get(done_idx, []),
                    }
                ),
            ).to_string()

        generated_assets_lists = await asyncio.gather(
            *async_assets_generation_tasks,
            return_exceptions=True,
        )
        generated_assets = []
        for assets_list in generated_assets_lists:
            if isinstance(assets_list, Exception):
                logger.error(
                    "Slide asset generation failed during final collection: %s",
                    assets_list,
                )
                continue
            generated_assets.extend(assets_list)

        for slide in slides:
            slide.ui = _apply_template_v2_content_to_ui(slide.ui, slide.content)

        # Moved this here to make sure new slides are generated before deleting the old ones
        await sql_session.execute(
            delete(SlideModel).where(SlideModel.presentation == id)
        )
        await sql_session.commit()

        sql_session.add(presentation)
        sql_session.add_all(slides)
        sql_session.add_all(generated_assets)
        await sql_session.commit()

        response = PresentationDetailWithSlides(
            **_presentation_response_data(presentation),
            slides=slides,
            fonts=await _resolve_presentation_fonts(presentation, slides, sql_session),
            merged_components=await _resolve_presentation_merged_components(
                presentation,
                slides,
                sql_session,
            ),
        )

        yield SSECompleteResponse(
            key="presentation",
            value=response.model_dump(mode="json"),
        ).to_string()

    async def rollback_stream_session():
        await sql_session.rollback()

    return StreamingResponse(
        safe_sse_stream(
            inner(),
            logger=logger,
            error_detail="Failed to generate presentation slides. Please try again.",
            on_error=rollback_stream_session,
        ),
        media_type="text/event-stream",
    )


@PRESENTATION_ROUTER.patch("/update", response_model=PresentationDetailWithSlides)
async def update_presentation(
    id: Annotated[uuid.UUID, Body()],
    n_slides: Annotated[Optional[int], Body()] = None,
    title: Annotated[Optional[str], Body()] = None,
    theme: Annotated[Optional[dict], Body()] = None,
    slides: Annotated[Optional[List[SlideModel]], Body()] = None,
    sql_session: AsyncSession = Depends(get_async_session),
):
    presentation = await sql_session.get(PresentationModel, id)
    if not presentation:
        raise HTTPException(status_code=404, detail="Presentation not found")

    presentation_update_dict = {}
    if n_slides is not None:
        if n_slides < 1:
            raise HTTPException(
                status_code=400,
                detail="Number of slides must be greater than 0",
            )
        if n_slides > MAX_NUMBER_OF_SLIDES:
            raise HTTPException(
                status_code=400,
                detail=f"Number of slides cannot be greater than {MAX_NUMBER_OF_SLIDES}",
            )
        presentation_update_dict["n_slides"] = n_slides
    if title:
        presentation_update_dict["title"] = title
    if theme or theme is None:
        presentation_update_dict["theme"] = theme

    if presentation_update_dict:
        presentation.sqlmodel_update(presentation_update_dict)
    if slides:
        if len(slides) > MAX_NUMBER_OF_SLIDES:
            raise HTTPException(
                status_code=400,
                detail=f"Number of slides cannot be greater than {MAX_NUMBER_OF_SLIDES}",
            )
        # Just to make sure id is UUID
        for slide in slides:
            slide.presentation = uuid.UUID(slide.presentation)
            slide.id = uuid.UUID(slide.id)

        await sql_session.execute(
            delete(SlideModel).where(SlideModel.presentation == presentation.id)
        )
        sql_session.add_all(slides)

    await sql_session.commit()

    response_slides = slides or []
    fonts = await _resolve_presentation_fonts(
        presentation,
        response_slides,
        sql_session,
    )
    merged_components = await _resolve_presentation_merged_components(
        presentation,
        response_slides,
        sql_session,
    )

    return PresentationDetailWithSlides(
        **_presentation_response_data(presentation),
        slides=response_slides,
        fonts=fonts,
        merged_components=merged_components,
    )


async def check_if_api_request_is_valid(
    request: GeneratePresentationRequest,
    sql_session: AsyncSession = Depends(get_async_session),
) -> Tuple[uuid.UUID,]:
    presentation_id = uuid.uuid4()
    print(f"Presentation ID: {presentation_id}")

    # Making sure either content, slides markdown, slides content or files is provided
    if not (
        request.content
        or request.slides_markdown
        or request.slides_content
        or request.files
    ):
        raise HTTPException(
            status_code=400,
            detail="Either content or slides markdown or files is required to generate presentation",
        )

    if request.n_slides is not None and request.n_slides <= 0:
        raise HTTPException(
            status_code=400,
            detail="Number of slides must be greater than 0",
        )

    if request.n_slides is not None and request.n_slides > MAX_NUMBER_OF_SLIDES:
        raise HTTPException(
            status_code=400,
            detail=f"Number of slides cannot be greater than {MAX_NUMBER_OF_SLIDES}",
        )

    if (
        request.slides_markdown is not None
        and len(request.slides_markdown) > MAX_NUMBER_OF_SLIDES
    ):
        raise HTTPException(
            status_code=400,
            detail=f"Number of slides cannot be greater than {MAX_NUMBER_OF_SLIDES}",
        )

    if (
        request.include_table_of_contents
        and request.n_slides is not None
        and request.n_slides < 3
    ):
        raise HTTPException(
            status_code=400,
            detail="Number of slides cannot be less than 3 if table of contents is included",
        )

    # Checking if template is valid — skipped when the caller supplies their
    # own inline layout payload (there is no stored template to resolve).
    if request.layout_payload is None and request.template not in DEFAULT_TEMPLATES:
        template_v2 = await _resolve_requested_template_v2(
            request.template,
            sql_session,
        )
        if not template_v2:
            raise HTTPException(
                status_code=400,
                detail="Template not found. Please use a valid template.",
            )
        request.template = f"template-v2-{template_v2.id}"
        return (presentation_id,)

    return (presentation_id,)


async def generate_presentation_handler(
    request: GeneratePresentationRequest,
    presentation_id: uuid.UUID,
    async_status: Optional[AsyncTaskModel],
    export_cookie_header: Optional[str] = None,
    sql_session: AsyncSession = Depends(get_async_session),
):
    try:
        using_slides_markdown = False
        using_slides_content = False
        content_adjustments: list[str] = []
        language_to_use = (request.language or "").strip() or None
        additional_context = ""

        if request.slides_content:
            if len(request.slides_content) > MAX_NUMBER_OF_SLIDES:
                raise HTTPException(
                    status_code=400,
                    detail=f"Number of slides cannot be greater than {MAX_NUMBER_OF_SLIDES}",
                )
            using_slides_content = True
            request.n_slides = len(request.slides_content)
        elif request.slides_markdown:
            using_slides_markdown = True
            if len(request.slides_markdown) > MAX_NUMBER_OF_SLIDES:
                raise HTTPException(
                    status_code=400,
                    detail=f"Number of slides cannot be greater than {MAX_NUMBER_OF_SLIDES}",
                )
            request.n_slides = len(request.slides_markdown)

        if not using_slides_markdown and not using_slides_content:
            # Updating async status
            if async_status:
                async_status.message = "Generating presentation outlines"
                async_status.data = _presentation_task_progress_data(
                    created_slides=0,
                    remaining_slides=_requested_slide_count(request),
                )
                async_status.updated_at = datetime.now()
                sql_session.add(async_status)
                await sql_session.commit()

            if request.files:
                documents_loader = DocumentsLoader(
                    file_paths=request.files,
                    presentation_language=request.language,
                )
                await documents_loader.load_documents()
                documents = documents_loader.documents
                if documents:
                    additional_context = "\n\n".join(documents)

            # Finding number of slides to generate by considering table of contents
            n_slides_to_generate = request.n_slides
            if request.include_table_of_contents and request.n_slides is not None:
                n_slides_to_generate = (
                    get_no_of_outlines_to_generate_for_n_slides(
                        n_slides=request.n_slides,
                        toc=True,
                        title_slide=request.include_title_slide,
                    )
                )

            outline_messages = get_outline_messages(
                request.content,
                n_slides_to_generate,
                language_to_use,
                additional_context,
                request.tone.value,
                request.verbosity.value,
                request.instructions,
                request.include_title_slide,
                request.include_table_of_contents,
            )
            await MEM0_PRESENTATION_MEMORY_SERVICE.store_generation_context(
                presentation_id=presentation_id,
                system_prompt=(
                    message_content_to_text(outline_messages[0].content)
                    if len(outline_messages) > 0
                    else None
                ),
                user_prompt=(
                    message_content_to_text(outline_messages[1].content)
                    if len(outline_messages) > 1
                    else None
                ),
                extracted_document_text=additional_context,
                source_content=request.content,
                instructions=request.instructions,
            )

            presentation_outlines_text = ""
            async for chunk in generate_ppt_outline(
                request.content,
                n_slides_to_generate,
                language_to_use,
                additional_context,
                request.tone.value,
                request.verbosity.value,
                request.instructions,
                request.include_title_slide,
                request.web_search,
                request.include_table_of_contents,
            ):

                if isinstance(chunk, HTTPException):
                    raise chunk

                presentation_outlines_text += chunk

            try:
                presentation_outlines_json = dict(
                    dirtyjson.loads(presentation_outlines_text)
                )
            except Exception:
                traceback.print_exc()
                raise HTTPException(
                    status_code=400,
                    detail="Failed to generate presentation outlines. Please try again.",
                )
            presentation_outlines = PresentationOutlineModel(
                **normalize_outline_payload(
                    presentation_outlines_json,
                    MAX_NUMBER_OF_SLIDES,
                )
            )

            if (
                n_slides_to_generate is not None
                and len(presentation_outlines.slides) != n_slides_to_generate
            ):
                raise HTTPException(
                    status_code=400,
                    detail=(
                        "Failed to generate presentation outlines with requested "
                        "number of slides. Please try again."
                    ),
                )

            total_outlines = len(presentation_outlines.slides)

        elif using_slides_markdown:
            # Setting outlines to slides markdown
            presentation_outlines = PresentationOutlineModel(
                slides=[
                    SlideOutlineModel(content=slide)
                    for slide in request.slides_markdown
                ]
            )
            total_outlines = len(request.slides_markdown)

            await MEM0_PRESENTATION_MEMORY_SERVICE.store_generation_context(
                presentation_id=presentation_id,
                system_prompt=None,
                user_prompt=None,
                extracted_document_text=None,
                source_content=request.content,
                instructions=request.instructions,
            )

        else:
            # Render-only mode: caller already authored every slide, so there
            # are no outlines to persist — keep a stub so downstream code that
            # touches presentation_outlines still works.
            presentation_outlines = PresentationOutlineModel(
                slides=[
                    SlideOutlineModel(content="")
                    for _ in request.slides_content
                ]
            )
            total_outlines = len(request.slides_content)

        await MEM0_PRESENTATION_MEMORY_SERVICE.store_generated_outlines(
            presentation_id,
            presentation_outlines.model_dump(mode="json"),
        )

        # Updating async status
        if async_status:
            async_status.message = "Selecting layout for each slide"
            async_status.updated_at = datetime.now()
            sql_session.add(async_status)
            await sql_session.commit()

        print("-" * 40)
        print(f"Generated {total_outlines} outlines for the presentation")

        logger.info(
            "[presentation.generate] loading layout template=%r presentation_id=%s",
            request.template,
            presentation_id,
        )
        if request.layout_payload is not None:
            # Inline template: the caller owns the component geometry, so the
            # payload also drives the per-slide `ui` rendering path.
            layout_payload = request.layout_payload
            layout_model = _build_template_v2_layout_model(
                layout_payload,
                layout_name="template-v2-inline",
            )
            template_fonts = None
            is_template_v2 = True
        else:
            (
                layout_payload,
                layout_model,
                template_fonts,
                is_template_v2,
            ) = await _resolve_generation_layout(request.template, sql_session)
        logger.info(
            "[presentation.generate] layout ready template=%r slides=%d ordered=%s icon_weight=%s",
            request.template,
            len(layout_model.slides),
            layout_model.ordered,
            layout_model.icon_weight,
        )
        total_slide_layouts = len(layout_model.slides)

        if using_slides_content:
            content_adjustments = _prepare_slides_content(
                layout_model, request.slides_content
            )

        # Generate Structure — skipped entirely when the caller supplied
        # per-slide contents (their layout_index picks the layout directly).
        if using_slides_content:
            presentation_structure = PresentationStructureModel(
                slides=[item.layout_index for item in request.slides_content]
            )
        elif layout_model.ordered:
            presentation_structure = layout_model.to_presentation_structure()
        else:
            presentation_structure: PresentationStructureModel = (
                await generate_presentation_structure(
                    presentation_outlines,
                    layout_model,
                    request.instructions,
                    using_slides_markdown,
                )
            )

        presentation_structure.slides = presentation_structure.slides[:total_outlines]
        for index in range(total_outlines):
            random_slide_index = random.randint(0, total_slide_layouts - 1)
            if index >= total_outlines:
                presentation_structure.slides.append(random_slide_index)
                continue
            if presentation_structure.slides[index] >= total_slide_layouts:
                presentation_structure.slides[index] = random_slide_index

        should_include_toc = (
            request.include_table_of_contents
            and not using_slides_markdown
            and not using_slides_content
        )
        if should_include_toc:
            n_toc_slides = get_no_of_toc_required_for_n_outlines(
                n_outlines=total_outlines,
                title_slide=request.include_title_slide,
                target_total_slides=request.n_slides,
            )
            toc_slide_layout_index = select_toc_or_list_slide_layout_index(layout_model)
            _insert_toc_layouts(
                presentation_structure,
                n_toc_slides,
                request.include_title_slide,
                toc_slide_layout_index,
            )
            if toc_slide_layout_index != -1 and n_toc_slides > 0:
                presentation_outlines = get_presentation_outline_model_with_toc(
                    outline=presentation_outlines,
                    n_toc_slides=n_toc_slides,
                    title_slide=request.include_title_slide,
                )

        final_n_slides = request.n_slides
        if final_n_slides is None:
            final_n_slides = len(presentation_outlines.slides)

        # Create PresentationModel
        presentation = PresentationModel(
            id=presentation_id,
            version=(
                PresentationVersion.V2_STANDARD
                if is_template_v2
                else PresentationVersion.V1_STANDARD
            ),
            theme=request.theme,
            content=request.content,
            n_slides=final_n_slides,
            language=language_to_use or "",
            title=get_presentation_title_from_presentation_outline(
                presentation_outlines
            ),
            outlines=presentation_outlines.model_dump(),
            layout=layout_payload,
            structure=presentation_structure.model_dump(),
            tone=request.tone.value,
            verbosity=request.verbosity.value,
            instructions=request.instructions,
            fonts=template_fonts,
        )

        # Updating async status
        if async_status:
            async_status.message = "Generating slides"
            async_status.data = _presentation_task_progress_data(
                created_slides=0,
                remaining_slides=final_n_slides or 0,
            )
            async_status.updated_at = datetime.now()
            sql_session.add(async_status)
            await sql_session.commit()

        image_generation_service = ImageGenerationService(get_images_directory())
        async_assets_generation_tasks = []
        image_warnings: List[dict] = []

        # 7. Generate slide content concurrently (batched), then build slides and fetch assets
        slides: List[SlideModel] = []

        slide_layout_indices = presentation_structure.slides
        slide_layouts = [layout_model.slides[idx] for idx in slide_layout_indices]
        total_slides_to_create = len(slide_layouts)

        # Schedule slide content generation and asset fetching in batches of 10
        batch_size = 10
        for start in range(0, len(slide_layouts), batch_size):
            end = min(start + batch_size, len(slide_layouts))

            print(f"Generating slides from {start} to {end}")

            if using_slides_content:
                # Caller authored the slide payloads; fold speaker notes into
                # the __speaker_note__ key the SlideModel builder reads.
                batch_contents = []
                for item in request.slides_content[start:end]:
                    slide_content = dict(item.content)
                    if item.speaker_note:
                        slide_content["__speaker_note__"] = item.speaker_note
                    batch_contents.append(slide_content)
            else:
                # Generate contents for this batch concurrently
                content_tasks = [
                    get_slide_content_from_type_and_outline(
                        slide_layouts[i],
                        presentation_outlines.slides[i],
                        language_to_use,
                        request.tone.value,
                        request.verbosity.value,
                        request.instructions,
                    )
                    for i in range(start, end)
                ]
                batch_contents: List[dict] = await asyncio.gather(*content_tasks)

            # Build slides for this batch
            batch_slides: List[SlideModel] = []
            for offset, slide_content in enumerate(batch_contents):
                i = start + offset
                slide_layout = slide_layouts[i]
                slide = SlideModel(
                    presentation=presentation_id,
                    layout_group=layout_model.name,
                    layout=slide_layout.id,
                    index=i,
                    speaker_note=slide_content.get("__speaker_note__"),
                    content=slide_content,
                    ui=(
                        _template_v2_slide_ui(layout_payload, slide_layout.id)
                        if is_template_v2
                        else None
                    ),
                )
                slides.append(slide)
                batch_slides.append(slide)

            if async_status:
                async_status.data = _presentation_task_progress_data(
                    created_slides=len(slides),
                    remaining_slides=total_slides_to_create - len(slides),
                )
                async_status.updated_at = datetime.now()
                sql_session.add(async_status)
                await sql_session.commit()

            if using_slides_markdown:
                image_urls_for_batch = get_images_for_slides_from_outline(
                    presentation_outlines.slides[start:end]
                )
            else:
                image_urls_for_batch = [[] for _ in batch_slides]

            # Start asset fetch tasks immediately so they run in parallel with next batch's LLM calls
            asset_tasks = [
                asyncio.create_task(
                    process_slide_and_fetch_assets(
                        image_generation_service,
                        slide,
                        outline_image_urls=image_urls_for_batch[offset],
                        icon_weight=layout_model.icon_weight,
                        allow_image_fallback=True,
                        image_warnings=image_warnings,
                    )
                )
                for offset, slide in enumerate(batch_slides)
            ]
            async_assets_generation_tasks.extend(asset_tasks)

        if async_status:
            async_status.message = "Fetching assets for slides"
            async_status.data = _presentation_task_progress_data(
                created_slides=len(slides),
                remaining_slides=0,
            )
            async_status.updated_at = datetime.now()
            sql_session.add(async_status)
            await sql_session.commit()

        # Run all asset tasks concurrently while batches may still be generating content
        generated_assets_list = await asyncio.gather(*async_assets_generation_tasks)
        generated_assets = []
        for assets_list in generated_assets_list:
            generated_assets.extend(assets_list)
        for warning in image_warnings:
            logger.warning(
                "Slide image generation warning: presentation_id=%s detail=%s",
                presentation_id,
                warning.get("detail"),
            )

        if is_template_v2:
            for slide in slides:
                _hydrate_template_v2_slide_ui(slide, layout_payload)

        # 8. Save PresentationModel and Slides
        sql_session.add(presentation)
        sql_session.add_all(slides)
        sql_session.add_all(generated_assets)
        await sql_session.commit()

        if async_status:
            async_status.message = "Exporting presentation"
            async_status.updated_at = datetime.now()
            sql_session.add(async_status)

        # 9. Export
        presentation_and_path = await export_presentation(
            presentation_id,
            presentation.title or str(uuid.uuid4()),
            request.export_as,
            cookie_header=export_cookie_header,
        )

        response = _attach_public_urls(
            PresentationPathAndEditPath(
                **presentation_and_path.model_dump(),
                edit_path=f"/presentation?id={presentation_id}",
                content_adjustments=content_adjustments or None,
            )
        )

        if async_status:
            async_status.message = "Presentation generation completed"
            async_status.status = "completed"
            async_status.data = _presentation_task_progress_data(
                created_slides=len(slides),
                remaining_slides=0,
            )
            async_status.updated_at = datetime.now()
            sql_session.add(async_status)
            await sql_session.commit()

        # Triggering webhook on success
        CONCURRENT_SERVICE.run_task(
            None,
            WebhookService.send_webhook,
            WebhookEvent.PRESENTATION_GENERATION_COMPLETED,
            response.model_dump(mode="json"),
        )

        return response

    except Exception as e:
        if not isinstance(e, HTTPException):
            traceback.print_exc()
            e = HTTPException(status_code=500, detail="Presentation generation failed")

        api_error_model = APIErrorModel.from_exception(e)

        # Triggering webhook on failure
        CONCURRENT_SERVICE.run_task(
            None,
            WebhookService.send_webhook,
            WebhookEvent.PRESENTATION_GENERATION_FAILED,
            api_error_model.model_dump(mode="json"),
        )

        if async_status:
            async_status.status = "error"
            async_status.message = "Presentation generation failed"
            async_status.updated_at = datetime.now()
            async_status.error = api_error_model.model_dump(mode="json")
            sql_session.add(async_status)
            await sql_session.commit()

        else:
            raise e


@PRESENTATION_ROUTER.post("/generate", response_model=PresentationPathAndEditPath)
async def generate_presentation_sync(
    request_http: Request,
    request: GeneratePresentationRequest,
    sql_session: AsyncSession = Depends(get_async_session),
):
    try:
        (presentation_id,) = await check_if_api_request_is_valid(request, sql_session)
        return await generate_presentation_handler(
            request,
            presentation_id,
            None,
            export_cookie_header=_build_export_cookie_header(request_http),
            sql_session=sql_session,
        )
    except HTTPException:
        raise
    except Exception:
        traceback.print_exc()
        raise HTTPException(status_code=500, detail="Presentation generation failed")


async def _run_generate_presentation_task(
    request: GeneratePresentationRequest,
    presentation_id: uuid.UUID,
    task_id: str,
    export_cookie_header: Optional[str],
) -> None:
    async with async_session_maker() as sql_session:
        async_status = await sql_session.get(AsyncTaskModel, task_id)
        if not async_status:
            logger.warning(
                "[presentation.generate.async] task missing task_id=%s",
                task_id,
            )
            return

        async_status.status = "processing"
        async_status.message = "Starting presentation generation"
        async_status.data = _presentation_task_progress_data(
            created_slides=0,
            remaining_slides=_requested_slide_count(request),
        )
        async_status.updated_at = datetime.now()
        sql_session.add(async_status)
        await sql_session.commit()

        await generate_presentation_handler(
            request,
            presentation_id,
            async_status=async_status,
            export_cookie_header=export_cookie_header,
            sql_session=sql_session,
        )


@PRESENTATION_ROUTER.post("/generate/async", response_model=AsyncTaskModel)
async def generate_presentation_async(
    request_http: Request,
    request: GeneratePresentationRequest,
    background_tasks: BackgroundTasks,
    sql_session: AsyncSession = Depends(get_async_session),
):
    try:
        (presentation_id,) = await check_if_api_request_is_valid(request, sql_session)

        async_status = AsyncTaskModel(
            type=ASYNC_TASK_TYPE_PRESENTATION_GENERATE,
            status="pending",
            message="Queued for generation",
            data=_presentation_task_progress_data(
                created_slides=0,
                remaining_slides=_requested_slide_count(request),
            ),
        )
        sql_session.add(async_status)
        await sql_session.commit()
        await sql_session.refresh(async_status)

        background_tasks.add_task(
            _run_generate_presentation_task,
            request,
            presentation_id,
            async_status.id,
            _build_export_cookie_header(request_http),
        )
        return async_status

    except Exception as e:
        if not isinstance(e, HTTPException):
            print(e)
            e = HTTPException(status_code=500, detail="Presentation generation failed")

        raise e


@PRESENTATION_ROUTER.get("/status/{id}", response_model=AsyncTaskModel)
async def check_async_presentation_generation_status(
    id: str = Path(description="ID of the presentation generation task"),
    sql_session: AsyncSession = Depends(get_async_session),
):
    status = await sql_session.get(AsyncTaskModel, id)
    if not status:
        raise HTTPException(
            status_code=404, detail="No presentation generation task found"
        )
    return status


@PRESENTATION_ROUTER.post("/edit", response_model=PresentationPathAndEditPath)
async def edit_presentation_with_new_content(
    request_http: Request,
    data: Annotated[EditPresentationRequest, Body()],
    sql_session: AsyncSession = Depends(get_async_session),
):
    presentation = await sql_session.get(PresentationModel, data.presentation_id)
    if not presentation:
        raise HTTPException(status_code=404, detail="Presentation not found")

    slides = await sql_session.scalars(
        select(SlideModel).where(SlideModel.presentation == data.presentation_id)
    )

    new_slides = []
    slides_to_delete = []
    for each_slide in slides:
        updated_content = None
        new_slide_data = list(
            filter(lambda x: x.index == each_slide.index, data.slides)
        )
        if new_slide_data:
            updated_content = deep_update(each_slide.content, new_slide_data[0].content)
            new_slide = each_slide.get_new_slide(presentation.id, updated_content)
            _hydrate_template_v2_slide_ui(new_slide, presentation.layout)
            new_slides.append(new_slide)
            slides_to_delete.append(each_slide.id)

    await sql_session.execute(
        delete(SlideModel).where(SlideModel.id.in_(slides_to_delete))
    )

    sql_session.add_all(new_slides)
    await sql_session.commit()

    presentation_and_path = await export_presentation(
        presentation.id,
        presentation.title or str(uuid.uuid4()),
        data.export_as,
        cookie_header=_build_export_cookie_header(request_http),
    )

    return _attach_public_urls(
        PresentationPathAndEditPath(
            **presentation_and_path.model_dump(),
            edit_path=f"/presentation?id={presentation.id}",
        )
    )


@PRESENTATION_ROUTER.post("/derive", response_model=PresentationPathAndEditPath)
async def derive_presentation_from_existing_one(
    request_http: Request,
    data: Annotated[EditPresentationRequest, Body()],
    sql_session: AsyncSession = Depends(get_async_session),
):
    presentation = await sql_session.get(PresentationModel, data.presentation_id)
    if not presentation:
        raise HTTPException(status_code=404, detail="Presentation not found")

    slides = await sql_session.scalars(
        select(SlideModel).where(SlideModel.presentation == data.presentation_id)
    )

    new_presentation = presentation.get_new_presentation()
    new_slides = []
    for each_slide in slides:
        updated_content = None
        new_slide_data = list(
            filter(lambda x: x.index == each_slide.index, data.slides)
        )
        if new_slide_data:
            updated_content = deep_update(each_slide.content, new_slide_data[0].content)
        new_slide = each_slide.get_new_slide(new_presentation.id, updated_content)
        _hydrate_template_v2_slide_ui(new_slide, new_presentation.layout)
        new_slides.append(new_slide)

    sql_session.add(new_presentation)
    sql_session.add_all(new_slides)
    await sql_session.commit()

    presentation_and_path = await export_presentation(
        new_presentation.id,
        new_presentation.title or str(uuid.uuid4()),
        data.export_as,
        cookie_header=_build_export_cookie_header(request_http),
    )

    return _attach_public_urls(
        PresentationPathAndEditPath(
            **presentation_and_path.model_dump(),
            edit_path=f"/presentation?id={new_presentation.id}",
        )
    )
