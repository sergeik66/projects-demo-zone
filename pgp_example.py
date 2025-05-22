import fsspec
import pgpy
import os

from spark_engine.common.email_util import get_secret_notebookutils

class PGP():
    def __init__(
            self,
            key_vault_name,
            file_system_code = "abfss",
            public_key_secret = None,
            private_key_secret = None,
            public_key = None,
            private_key = None
    ):
        
        storage_options = {
            "account_name": "onelake",
            "account_host": "onelake.dfs.fabric.microsoft.com",
        }
        self.onelake_fs = fsspec.filesystem(file_system_code, **storage_options)
        
        if public_key_secret:
            self.public_key = get_secret_notebookutils(public_key_secret, key_vault_name)
        else:
            self.public_key = public_key

        if private_key_secret:
            self.private_key = get_secret_notebookutils(private_key_secret, key_vault_name)
        else:
            self.private_key = private_key

        self.gpg = pgpy.PGPKey.from_blob(self.public_key)

        
    def encrypt_file(self, input_file, output_path):
        pubkey, _ = pgpy.PGPKey.from_blob(self.public_key)
        with self.onelake_fs.open(input_file, "rb") as rb_file:
            file_message = pgpy.PGPMessage.new(rb_file.read(), file=True)

        encrypted_message = pubkey.encrypt(file_message)

        file_name = os.path.basename(input_file)
        encrypted_path = f"{output_path}/{file_name}.pgp"
        with self.onelake_fs.open(encrypted_path, "w") as w_file:
            w_file.write(str(encrypted_message))
        
        return self
    
    def decrypt_file(self, input_file, output_path, passphrase = None):
        privkey, _ = pgpy.PGPKey.from_blob(self.private_key)
        with self.onelake_fs.open(input_file, "rb") as rb_file:
            enc_message = pgpy.PGPMessage.from_blob(rb_file.read())

        if privkey.is_protected:
            with privkey.unlock(passphrase):
                decrypted_message = privkey.decrypt(enc_message).message
        else:
            decrypted_message = privkey.decrypt(enc_message).message

        file_name = os.path.basename(input_file)
        file_name = file_name[:-4] if file_name[-4:] == ".pgp" else file_name
        decrypted_path = f"{output_path}/{file_name}"

        mode = "w"
        if isinstance(decrypted_message, (bytes, bytearray)):
            mode = "wb"
        
        with self.onelake_fs.open(decrypted_path, mode) as w_file:
            w_file.write(decrypted_message)
