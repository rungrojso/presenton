"""Hand-written MCP surface for Presenton (fork patch).

Upstream builds the MCP server with FastMCP.from_openapi — an auto-generated
proxy whose tool schemas are thin and whose responses keep whatever envelope
the REST endpoint returned. This module replaces it with explicit tools so
the contract is deliberate:

  list_templates        -> GET  /api/v1/ppt/template/all (items carry
                           `template_arg` — the exact string to pass as
                           `template` to generate_presentation)
  generate_presentation -> POST /api/v1/ppt/presentation/generate
                           -> {presentation_id, path, edit_path,
                              download_url?, edit_url?}
  get_presentation      -> GET  /api/v1/ppt/presentation/{id}
  get_generation_status -> GET  /api/v1/ppt/presentation/status/{id}

Auth plumbing (PresentonTokenVerifier, bearer forwarding) is unchanged.
"""
import sys
import argparse
import asyncio
import traceback

import httpx
from fastmcp import FastMCP
from fastmcp.server.auth import AccessToken, TokenVerifier
from fastmcp.server.dependencies import get_access_token, get_http_headers

from utils.get_env import is_disable_auth_enabled, is_presenton_electron_desktop
from utils.simple_auth import is_auth_configured, validate_session_token

MCP_API_BASE_URL = "http://127.0.0.1:8000"
# Presentation generation can take several minutes; keep MCP upstream reads open.
MCP_API_TIMEOUT_SECONDS = 600.0
MCP_API_CONNECT_TIMEOUT_SECONDS = 15.0


class PresentonTokenVerifier(TokenVerifier):
    """Validate Presenton session tokens for MCP HTTP auth."""

    async def verify_token(self, token: str) -> AccessToken | None:
        username = validate_session_token(token)
        if not username:
            return None

        return AccessToken(
            token=token,
            client_id=username,
            scopes=[],
            claims={"u": username},
        )


def is_mcp_server_enabled() -> bool:
    """MCP is only supported in server/Docker deployments, not the Electron app."""
    return not is_presenton_electron_desktop()


def create_mcp_auth_provider() -> TokenVerifier | None:
    """Enable MCP bearer auth only when app auth is configured."""
    if is_disable_auth_enabled() or not is_auth_configured():
        return None
    return PresentonTokenVerifier()


def get_mcp_api_timeout() -> httpx.Timeout:
    return httpx.Timeout(
        timeout=MCP_API_TIMEOUT_SECONDS,
        connect=MCP_API_CONNECT_TIMEOUT_SECONDS,
    )


def create_api_client() -> httpx.AsyncClient:
    return httpx.AsyncClient(
        base_url=MCP_API_BASE_URL,
        timeout=get_mcp_api_timeout(),
        event_hooks={"request": [attach_request_auth_header]},
    )


async def attach_request_auth_header(request: httpx.Request) -> None:
    """Forward the authenticated MCP caller token to FastAPI tool endpoints."""
    if "authorization" in request.headers:
        return

    access_token = get_access_token()
    if access_token:
        request.headers["Authorization"] = f"Bearer {access_token.token}"
        return

    forwarded_headers = get_http_headers(include={"authorization"})
    incoming_auth_header = forwarded_headers.get("authorization")
    if incoming_auth_header:
        request.headers["Authorization"] = incoming_auth_header


def create_mcp_server(name: str = "Presenton") -> FastMCP:
    """Explicit tool surface — one tool per operation, responses unwrapped."""
    mcp = FastMCP(name, auth=create_mcp_auth_provider())

    @mcp.tool()
    async def list_templates() -> list[dict]:
        """List available presentation templates.

        Each item has `template_arg` — pass that string verbatim as the
        `template` argument of generate_presentation (`custom-<uuid>` for
        uploaded/seeded templates, a group name for built-ins).
        """
        async with create_api_client() as client:
            resp = await client.get("/api/v1/ppt/template/all")
            resp.raise_for_status()
            return resp.json()

    @mcp.tool()
    async def generate_presentation(
        content: str,
        template: str = "general",
        n_slides: int | None = None,
        language: str | None = None,
        export_as: str = "pptx",
        instructions: str | None = None,
        slides_markdown: list[str] | None = None,
        tone: str = "default",
        verbosity: str = "standard",
        web_search: bool = False,
        include_title_slide: bool = True,
        include_table_of_contents: bool = False,
    ) -> dict:
        """Generate a presentation and return its file + editor links.

        `template`: use a `template_arg` from list_templates.
        `slides_markdown`: supply ready-made per-slide markdown to skip
        outline generation (fewer model calls, deterministic structure).
        Response includes `download_url`/`edit_url` (absolute, browser-ready)
        when the server has PUBLIC_BASE_URL configured — always prefer those
        when showing links to users; `path`/`edit_path` are container-relative.
        `artifact_html` is a ready-made <iframe> snippet: emit it inside a
        fenced ```html block so the chat client renders the deck in its
        artifact/preview panel.
        """
        payload = {
            "content": content,
            "template": template,
            "export_as": export_as,
            "tone": tone,
            "verbosity": verbosity,
            "web_search": web_search,
            "include_title_slide": include_title_slide,
            "include_table_of_contents": include_table_of_contents,
            "trigger_webhook": False,
        }
        for key, value in (
            ("n_slides", n_slides),
            ("language", language),
            ("instructions", instructions),
            ("slides_markdown", slides_markdown),
        ):
            if value is not None:
                payload[key] = value

        async with create_api_client() as client:
            resp = await client.post("/api/v1/ppt/presentation/generate", json=payload)
            resp.raise_for_status()
            result = resp.json()

        # artifact_html: drop-in snippet clients can render in a side-panel
        # (LibreChat artifacts render fenced ```html blocks verbatim).
        frame_src = result.get("edit_url") or result.get("edit_path") or result.get("path")
        if frame_src:
            result["artifact_html"] = (
                f'<iframe src="{frame_src}" '
                'style="width:100%;height:640px;border:none;border-radius:8px" '
                'allowfullscreen></iframe>'
            )
        return result

    @mcp.tool()
    async def get_presentation(presentation_id: str) -> dict:
        """Fetch a presentation with its slides (structured slide content)."""
        async with create_api_client() as client:
            resp = await client.get(f"/api/v1/ppt/presentation/{presentation_id}")
            resp.raise_for_status()
            return resp.json()

    @mcp.tool()
    async def get_generation_status(presentation_id: str) -> dict:
        """Poll the async task status of a presentation generation."""
        async with create_api_client() as client:
            resp = await client.get(
                f"/api/v1/ppt/presentation/status/{presentation_id}")
            resp.raise_for_status()
            return resp.json()

    return mcp


async def main():
    try:
        if not is_mcp_server_enabled():
            print(
                "INFO: MCP server is disabled in the Presenton Electron desktop app "
                "(PRESENTON_ELECTRON=true)."
            )
            return

        parser = argparse.ArgumentParser(description="Run the Presenton MCP server")
        parser.add_argument(
            "--port", type=int, default=8001, help="Port for the MCP HTTP server"
        )
        parser.add_argument(
            "--name", type=str, default="Presenton", help="MCP server display name"
        )
        args = parser.parse_args()

        mcp = create_mcp_server(args.name)

        print(f"DEBUG: Starting explicit MCP server on 127.0.0.1:{args.port}")
        await mcp.run_async(
            transport="http",
            host="127.0.0.1",
            port=args.port,
            uvicorn_config={"reload": True},
        )
    except Exception as e:
        print(f"ERROR: MCP server startup failed: {e}")
        print(f"ERROR: Traceback: {traceback.format_exc()}")
        raise


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except Exception as e:
        print(f"FATAL ERROR: {e}")
        print(f"FATAL TRACEBACK: {traceback.format_exc()}")
        sys.exit(1)
