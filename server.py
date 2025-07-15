import os
import uuid
import logging
from typing import Any, Dict

from mcp.server.fastmcp import FastMCP
from temporalio.client import Client

from workflows import InvoiceWorkflow

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


async def _safe_info(context: Any, message: str) -> None:
    if getattr(context, "info", None):
        await context.info(message)


async def _safe_progress(context: Any, current: int, total: int) -> None:
    if getattr(context, "report_progress", None):
        await context.report_progress(current, total)


async def _client() -> Client:
    return await Client.connect(os.getenv("TEMPORAL_ADDRESS", "localhost:7233"))


mcp = FastMCP("invoice_processor")


@mcp.tool()
async def process_invoice(
    invoice: Dict, *, context: Any | None = None
) -> Dict[str, str]:
    """Start the InvoiceWorkflow with the given invoice JSON."""
    logger.info("Processing invoice %s", invoice.get("invoice_id"))
    await _safe_info(context, "Starting invoice workflow...")
    await _safe_progress(context, 10, 100)
    client = await _client()
    handle = await client.start_workflow(
        InvoiceWorkflow.run,
        invoice,
        id=f"invoice-{uuid.uuid4()}",
        task_queue="invoice-task-queue",
    )
    logger.info("Started workflow %s", handle.id)
    await _safe_info(context, f"Started workflow {handle.id}")
    await _safe_progress(context, 100, 100)
    return {"workflow_id": handle.id, "run_id": handle.result_run_id}


@mcp.tool()
async def approve_invoice(
    workflow_id: str, run_id: str, *, context: Any | None = None
) -> str:
    """Signal approval for the invoice workflow."""
    logger.info("Approving invoice workflow %s", workflow_id)
    await _safe_info(context, "Sending approve signal...")
    await _safe_progress(context, 50, 100)
    client = await _client()
    handle = client.get_workflow_handle(workflow_id=workflow_id, run_id=run_id)
    await handle.signal("ApproveInvoice")
    await _safe_info(context, "Approve signal sent")
    await _safe_progress(context, 100, 100)
    logger.info("Approve signal sent for %s", workflow_id)
    return "APPROVED"


@mcp.tool()
async def reject_invoice(
    workflow_id: str, run_id: str, *, context: Any | None = None
) -> str:
    """Signal rejection for the invoice workflow."""
    logger.info("Rejecting invoice workflow %s", workflow_id)
    await _safe_info(context, "Sending reject signal...")
    await _safe_progress(context, 50, 100)
    client = await _client()
    handle = client.get_workflow_handle(workflow_id=workflow_id, run_id=run_id)
    await handle.signal("RejectInvoice")
    await _safe_info(context, "Reject signal sent")
    await _safe_progress(context, 100, 100)
    logger.info("Reject signal sent for %s", workflow_id)
    return "REJECTED"


@mcp.tool()
async def invoice_status(
    workflow_id: str, run_id: str, *, context: Any | None = None
) -> str:
    """Return current status of the workflow."""
    logger.info("Checking status for workflow %s", workflow_id)
    await _safe_info(context, "Fetching workflow status...")
    await _safe_progress(context, 30, 100)
    client = await _client()
    handle = client.get_workflow_handle(workflow_id=workflow_id, run_id=run_id)
    desc = await handle.describe()
    status = await handle.query("GetInvoiceStatus")
    await _safe_info(context, f"Status: {status}")
    await _safe_progress(context, 100, 100)
    logger.info("Workflow %s status %s", workflow_id, status)
    return (
        f"Invoice with ID {workflow_id} is currently {status}. "
        f"Workflow status: {desc.status.name}"
    )


if __name__ == "__main__":
    mcp.run(transport="stdio")
