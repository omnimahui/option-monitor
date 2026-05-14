"""
Run this script once (or whenever a session expires) to authenticate each
Fidelity account and save the browser session to disk.

After a successful run, tda.py can fetch positions silently with no browser
window and no 2FA prompts.

Usage:
    python fidelity-login.py
"""

import os
import sys

sys.path.insert(0, os.path.dirname(__file__))
from settings import FIDELITY_CREDENTIALS
from fidelity.fidelity import FidelityAutomation

curr_dir = os.path.dirname(os.path.abspath(__file__))

LOGIN_URL   = "https://digital.fidelity.com/prgw/digital/login/full-page"
SUMMARY_URL = "https://digital.fidelity.com/ftgw/digital/portfolio/summary"
TIMEOUT_MS  = 5 * 60 * 1000   # 5 minutes for user to complete login + 2FA


def login_account(idx: int, username: str, password: str) -> bool:
    print(f"\n[{idx+1}/{len(FIDELITY_CREDENTIALS)}] Opening browser for {username} ...")

    fid = FidelityAutomation(
        headless=False,
        save_state=True,
        title=f"acct{idx}",
        profile_path=curr_dir,
    )
    try:
        fid.page.goto(LOGIN_URL, timeout=60_000)

        # Pre-fill credentials; ignore errors (saved session may skip login page)
        try:
            fid.page.get_by_label("Username", exact=True).fill(username)
            fid.page.get_by_label("Password", exact=True).fill(password)
            fid.page.get_by_role("button", name="Log in").click()
        except Exception:
            pass

        print(f"  Complete any 2FA / security prompts in the browser window.")
        print(f"  Waiting up to 5 minutes for you to reach the portfolio summary page ...")

        fid.page.wait_for_url(SUMMARY_URL, timeout=TIMEOUT_MS)

        # Session is now valid — persist it
        fid.save_storage_state()
        print(f"  Logged in. Fetching account info ...")

        account_dict = fid.getAccountInfo()
        if account_dict:
            print(f"  OK — {len(account_dict)} account(s): {list(account_dict.keys())}")
        else:
            print(f"  WARNING: session saved but getAccountInfo() returned nothing.")

        session_file = os.path.join(curr_dir, f"Fidelity_acct{idx}.json")
        print(f"  Session saved to {session_file}")
        return True

    except Exception as e:
        print(f"  ERROR: {e}")
        return False
    finally:
        fid.close_browser()


def main():
    active = [(i, u, p) for i, (u, p) in enumerate(FIDELITY_CREDENTIALS) if u and p]
    if not active:
        print("No credentials found in FIDELITY_CREDENTIALS in settings.py")
        return

    results = []
    for idx, username, password in active:
        ok = login_account(idx, username, password)
        results.append((username, ok))

    print("\n=== Summary ===")
    for username, ok in results:
        print(f"  {username}: {'OK' if ok else 'FAILED'}")

    if all(ok for _, ok in results):
        print("\nAll sessions saved. You can now run tda.py without interruption.")
    else:
        print("\nSome logins failed. Fix the issues above and re-run this script.")


if __name__ == "__main__":
    main()
