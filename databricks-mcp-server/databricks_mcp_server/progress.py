"""
Progress Notification Support
==============================

Provides a lightweight ``ProgressReporter`` that tool handlers can use to
emit MCP ``notifications/progress`` messages during long-running operations.

If the client did not supply a ``progressToken`` in the request's ``_meta``,
the reporter silently no-ops so that callers never need to check for ``None``.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import Any

from mcp.server.session import ServerSession
from mcp.shared.context import RequestContext

logger = logging.getLogger(__name__)


@dataclass
class ProgressReporter:
    """Emit MCP progress notifications within a tool handler.

    Parameters
    ----------
    session : ServerSession | None
        The active server session (used to send notifications).
    progress_token : str | int | None
        Opaque token supplied by the client.  When ``None``, all calls
        to :meth:`report` are no-ops.
    """

    session: ServerSession | None = None
    progress_token: str | int | None = None
    _step: int = field(default=0, init=False, repr=False)

    # ------------------------------------------------------------------
    # Factory
    # ------------------------------------------------------------------
    @classmethod
    def from_request_context(cls, ctx: RequestContext[Any, Any, Any]) -> ProgressReporter:
        """Create a reporter from the current MCP request context."""
        token = ctx.meta.progressToken if ctx.meta else None
        return cls(session=ctx.session, progress_token=token)

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------
    async def report(
        self,
        message: str,
        progress: float,
        total: float | None = None,
    ) -> None:
        """Send a progress notification (no-op when the client did not
        request progress updates).

        Parameters
        ----------
        message : str
            Human-readable status update.
        progress : float
            Current progress value (should increase monotonically).
        total : float | None
            Total expected value, if known.
        """
        if self.progress_token is None or self.session is None:
            return

        try:
            await self.session.send_progress_notification(
                progress_token=self.progress_token,
                progress=progress,
                total=total,
                message=message,
            )
        except Exception:
            # Progress notifications are best-effort; never let a
            # notification failure break the actual tool operation.
            logger.debug("Failed to send progress notification", exc_info=True)
