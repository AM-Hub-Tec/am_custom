import frappe
from frappe.utils import nowdate

PAYGO_DOCTYPE = "Paygo Contract"

def process_mpesa_payment(mpesa_payment_name: str):
    mp = frappe.get_doc("Mpesa Payment", mpesa_payment_name)

    amount = float(mp.amount_paid or 0)
    if amount <= 0:
        return

    mpesa_ref = (mp.mpesa_ref or "").strip()
    national_id = (mp.account_ref or "").strip()

    if mpesa_ref and frappe.db.exists("Payment Entry", {"reference_no": mpesa_ref, "docstatus": ["!=", 2]}):
        _try_link_contract_only(mp)
        return

    contract = None
    if national_id:
        contract = frappe.db.get_value(
            PAYGO_DOCTYPE,
            {"national_id": national_id, "contract_status": "Active"},
            "name"
        )

    if not contract:
        mp.linked_doctype = ""
        mp.linked_name = ""
        mp.save(ignore_permissions=True)
        frappe.db.commit()
        return

    pc = frappe.get_doc(PAYGO_DOCTYPE, contract)

    pc.total_paid = float(pc.total_paid or 0) + amount
    pc.cash_price = float(pc.cash_price or 0) + amount
    pc.save(ignore_permissions=True)

    customer = getattr(pc, "customer", None)

    if customer:
        _create_payment_entry(customer, amount, mpesa_ref)

    mp.linked_doctype = PAYGO_DOCTYPE
    mp.linked_name = contract
    mp.save(ignore_permissions=True)
    frappe.db.commit()


def _create_payment_entry(customer: str, amount: float, mpesa_ref: str):
    MOP_NAME = "M-PESA"

    invoices = frappe.db.sql("""
        SELECT name, outstanding_amount, posting_date
        FROM `tabSales Invoice`
        WHERE docstatus = 1
          AND customer = %s
          AND outstanding_amount > 0
        ORDER BY posting_date ASC, name ASC
    """, (customer,), as_dict=True)

    from erpnext.accounts.doctype.payment_entry.payment_entry import get_payment_entry

    if invoices:
        pe = get_payment_entry("Sales Invoice", invoices[0]["name"])
    else:
        pe = frappe.get_doc({
            "doctype": "Payment Entry",
            "payment_type": "Receive",
            "party_type": "Customer",
            "party": customer,
            "posting_date": nowdate(),
        })

    pe.mode_of_payment = MOP_NAME
    pe.reference_no = mpesa_ref
    pe.reference_date = nowdate()
    pe.paid_amount = amount
    pe.received_amount = amount

    remaining = amount
    if invoices and hasattr(pe, "references"):
        pe.references = []
        for inv in invoices:
            if remaining <= 0:
                break
            out = float(inv.outstanding_amount or 0)
            if out <= 0:
                continue
            alloc = out if out <= remaining else remaining
            pe.append("references", {
                "reference_doctype": "Sales Invoice",
                "reference_name": inv["name"],
                "allocated_amount": alloc
            })
            remaining -= alloc

    pe.insert(ignore_permissions=True)
    pe.submit()


def _try_link_contract_only(mp):
    national_id = (mp.account_ref or "").strip()
    if not national_id:
        return
    contract = frappe.db.get_value(
        PAYGO_DOCTYPE,
        {"national_id": national_id, "contract_status": "Active"},
        "name"
    )
    if contract:
        mp.linked_doctype = PAYGO_DOCTYPE
        mp.linked_name = contract
        mp.save(ignore_permissions=True)
        frappe.db.commit()
