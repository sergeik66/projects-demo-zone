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

pgp = PGP(
    key_vault_name="my-key-vault",
    public_key_secret="mock-public-key-secret",
    private_key_secret="mock-private-key-secret",
    passphrase_secret="mock-passphrase-secret"
)
input_file = "abfss://.../Files/user_data/pdos_webreport/decrypted/BOP PD & OS.xlsx"
output_path = "abfss://.../Files/user_data/pdos_webreport/encrypted"
pgp.encrypt_file(input_file, output_path)  # Re-encrypt to ensure key match
