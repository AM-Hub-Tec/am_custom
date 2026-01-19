import frappe
import json

@frappe.whitelist(allow_guest=True)
def at_mpesa_callback():
    raw = frappe.request.get_data(as_text=True) or ""
    try:
        payload = json.loads(raw) if raw else {}
    except Exception:
        payload = {"_raw": raw}

    mpesa_ref   = (payload.get("transactionId") or payload.get("mpesa_ref") or "").strip()
    account_ref = (payload.get("accountNumber") or payload.get("account_ref") or "").strip()
    phone_no    = (payload.get("phoneNumber") or payload.get("phone_no") or "").strip()
    amount_paid = payload.get("amount") or payload.get("amount_paid") or 0
    trans_time  = payload.get("transactionTime") or payload.get("trans_time")

    if not mpesa_ref:
        frappe.log_error(raw, "AT Callback missing mpesa_ref")
        return {"status": "ok"}

    if frappe.db.exists("Mpesa Payment", {"mpesa_ref": mpesa_ref}):
        return {"status": "ok", "message": "duplicate_ignored"}

    doc = frappe.get_doc({
        "doctype": "Mpesa Payment",
        "mpesa_ref": mpesa_ref,
        "account_ref": account_ref,
        "amount_paid": float(amount_paid) if amount_paid else 0,
        "phone_no": phone_no,
        "trans_time": trans_time,
        "raw_payload": raw,
        "source": "Africa's Talking",
        "linked_doctype": "",
        "linked_name": ""
    })
    doc.insert(ignore_permissions=True)
    frappe.db.commit()

    frappe.enqueue(
        "am_custom.api.mpesa_processor.process_mpesa_payment",
        mpesa_payment_name=doc.name,
        queue="short"
    )

    return {"status": "ok"}
