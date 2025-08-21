from __future__ import annotations

from datetime import datetime, timedelta

from temporalio import workflow
from temporalio.common import RetryPolicy
from temporalio.exceptions import ActivityError, ApplicationError

from activities import validate_against_erp, payment_gateway


def _parse_due_date(due: str) -> datetime:
    if due.endswith("Z"):
        due = due[:-1] + "+00:00"
    return datetime.fromisoformat(due)


@workflow.defn
class PayLineItem:
    @workflow.run
    async def run(self, line: dict) -> str:
        due = _parse_due_date(line["due_date"])
        delay = (due - workflow.now()).total_seconds()
        if delay > 0:
            await workflow.sleep(delay)
        try:
            await workflow.execute_activity(
                payment_gateway,
                line,
                start_to_close_timeout=timedelta(seconds=30),
                retry_policy=RetryPolicy(
                    initial_interval=timedelta(seconds=1),
                    maximum_interval=timedelta(seconds=30),
                    maximum_attempts=3,
                    non_retryable_error_types=["INSUFFICIENT_FUNDS"],
                ),
            )
            return "SUCCESS"
        except ActivityError as e:
            workflow.logger.warning(f"Payment failed for line item {line}: {e}")
            return f"ERROR-{e.cause.message}" if e.cause else "ERROR"


@workflow.defn
class InvoiceWorkflow:
    def __init__(self) -> None:
        self.approved: bool | None = None
        self.status: str = "INITIALIZING"

    @workflow.signal
    async def ApproveInvoice(self) -> None:
        self.approved = True

    @workflow.signal
    async def RejectInvoice(self) -> None:
        self.approved = False

    @workflow.query
    async def IsInvoiceApproved(self) -> bool:
        if self.approved is None:
            raise ApplicationError("Invoice approval status is not set yet.")
        return self.approved

    @workflow.query
    async def GetInvoiceStatus(self) -> str:
        return self.status

    @workflow.run
    async def run(self, invoice: dict) -> str:
        self.status = "PENDING-VALIDATION"
        workflow.logger.info(
            f"Starting workflow for invoice {invoice.get('invoice_id')}"
        )
        await workflow.execute_activity(
            validate_against_erp,
            invoice,
            start_to_close_timeout=timedelta(seconds=30),
            retry_policy=RetryPolicy(
                initial_interval=timedelta(seconds=1),
                maximum_interval=timedelta(seconds=30),
                maximum_attempts=5,
            ),
        )

        self.status = "PENDING-APPROVAL"
        workflow.logger.info(
            f"Waiting for approval for invoice {invoice.get('invoice_id')}"
        )
        # Wait for the approval signal

        await workflow.wait_condition(
            lambda: self.approved is not None,
            timeout=timedelta(days=5),
        )

        if not self.approved:
            workflow.logger.info("REJECTED")
            self.status = "REJECTED"
            return "REJECTED"

        self.status = "APPROVED"
        workflow.logger.info(
            f"Invoice {invoice.get('invoice_id')} approved, processing line items"
        )
        # Process each line item in parallel
        results = []
        for line in invoice.get("lines", []):
            handle = await workflow.start_child_workflow(
                PayLineItem.run,
                line,
            )
            workflow.logger.info(
                f"Started child workflow for line item {line} with handle {handle}"
            )
            results.append(handle)

        workflow.logger.info(f"Waiting for {len(results)} child workflows to complete")
        self.status = "PAYING"
        failedcount = 0
        for handle in results:
            try:
                await handle
                wf_result = handle.result()
                workflow.logger.warning(
                    f"Child workflow completed with result: {wf_result}"
                )
            except Exception as e:
                workflow.logger.warning(f"Child workflow failed with exception: {e}")
                wf_result = "ERROR"

            workflow.logger.info(f"Child workflow result: {wf_result}")
            if wf_result is None:
                workflow.logger.warning("Child workflow returned None, there's a bug")
                failedcount += 1
                self.status = "FAILED"
            elif wf_result == "ERROR":
                workflow.logger.warning("LINE ITEM PAYMENT ERROR")
                failedcount += 1
                self.status = "FAILED"
            elif wf_result == "SUCCESS":
                workflow.logger.info("LINE ITEM PAID SUCCESSFULLY")

        if failedcount > 0:
            workflow.logger.warning(f"{failedcount} line items failed to pay")
            self.status = "FAILED"
        else:
            workflow.logger.info("All line items paid successfully")
            self.status = "PAID"
        return self.status
