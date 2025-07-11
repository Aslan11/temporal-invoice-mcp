import os
import uuid
from typing import Dict

from mcp.server.fastmcp import FastMCP
from temporalio.client import Client

from workflows import InvoiceWorkflow


async def _client() -> Client:
    return await Client.connect(os.getenv("TEMPORAL_ADDRESS", "localhost:7233"))


mcp = FastMCP("invoice_processor")


@mcp.tool()
async def process_invoice(invoice: Dict) -> Dict[str, str]:
    """Start the InvoiceWorkflow with the given invoice JSON."""
    client = await _client()
    handle = await client.start_workflow(
        InvoiceWorkflow.run,
        invoice,
        id=f"invoice-{uuid.uuid4()}",
        task_queue="invoice-task-queue",
    )
    return {"workflow_id": handle.id, "run_id": handle.result_run_id}


@mcp.tool()
async def approve_invoice(workflow_id: str, run_id: str) -> str:
    """Signal approval for the invoice workflow."""
    client = await _client()
    handle = client.get_workflow_handle(workflow_id=workflow_id, run_id=run_id)
    await handle.signal("ApproveInvoice")
    return "APPROVED"


@mcp.tool()
async def reject_invoice(workflow_id: str, run_id: str) -> str:
    """Signal rejection for the invoice workflow."""
    client = await _client()
    handle = client.get_workflow_handle(workflow_id=workflow_id, run_id=run_id)
    await handle.signal("RejectInvoice")
    return "REJECTED"


@mcp.tool()
async def invoice_status(workflow_id: str, run_id: str) -> str:
    """Return current status of the workflow."""
    client = await _client()
    handle = client.get_workflow_handle(workflow_id=workflow_id, run_id=run_id)
    desc = await handle.describe()
    status = await handle.query("GetInvoiceStatus")
    return f"Invoice with ID {workflow_id} is currently {status}. " \
           f"Workflow status: {desc.status.name}"


if __name__ == "__main__":
    mcp.run(transport="stdio")
