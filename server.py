import os
import uuid
from typing import Dict, Any

from mcp.server.fastmcp import FastMCP
from temporalio.client import Client

from workflows import InvoiceWorkflow


async def _client() -> Client:
    return await Client.connect(os.getenv("TEMPORAL_ADDRESS", "localhost:7233"))


mcp = FastMCP("invoice_processor")


@mcp.prompt()
async def process_invoice_prompt() -> Dict[str, Any]:
    """Prompt the user to submit invoice JSON for processing."""
    return {
        "tool": "process_invoice",
        "fields": [
            {
                "name": "invoice",
                "type": "textarea",
                "label": "Invoice JSON",
            }
        ],
    }


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


@mcp.prompt()
async def approve_invoice_prompt() -> str:
    """Guide the user to approve a workflow."""
    return (
        "Call the 'approve_invoice' tool with the workflow_id and run_id to "
        "approve the invoice."
    )


@mcp.prompt()
async def reject_invoice_prompt() -> str:
    """Guide the user to reject a workflow."""
    return (
        "Call the 'reject_invoice' tool with the workflow_id and run_id to "
        "reject the invoice."
    )


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
