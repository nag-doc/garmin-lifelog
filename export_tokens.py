import os
import sys
import getpass
from garminconnect import Garmin
import garth

print("="*60)
print("  Garmin トークンエクスポートツール")
print("="*60)

email = input("Email: ")
password = getpass.getpass("Password: ")

try:
    garmin = Garmin(email, password, is_cn=False, prompt_mfa=lambda: input("MFA Code: "))
    garmin.login()
    print("\n" + "="*60)
    print("↓ 以下の文字列をすべてコピーしてください (GARMIN_TOKENS) ↓")
    print("="*60 + "\n")
    print(garmin.garth.dumps())
    print("\n" + "="*60 + "\n")
except Exception as e:
    print(f"\nError: ログインに失敗しました: {e}")
