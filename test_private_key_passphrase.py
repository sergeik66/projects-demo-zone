import pgpy
from spark_engine.common.email_util import get_secret_notebookutils
private_key_str = get_secret_notebookutils("mock-private-key-secret", "my-key-vault")
passphrase = get_secret_notebookutils("mock-passphrase-secret", "my-key-vault")
privkey, _ = pgpy.PGPKey.from_blob(private_key_str)
try:
    with privkey.unlock(passphrase):
        print("Private key unlocked successfully")
except pgpy.errors.PGPError as e:
    print(f"Passphrase error: {str(e)}")
