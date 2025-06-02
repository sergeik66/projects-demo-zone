import fsspec
import pgpy
import os
from typing import Optional
from spark_engine.common.email_util import get_secret_notebookutils

class PGP:
    """A class to handle PGP encryption/decryption of files using fsspec and pgpy."""
    def __init__(
        self,
        key_vault_name: str,
        file_system_code: str = "abfss",
        public_key_secret: Optional[str] = None,
        private_key_secret: Optional[str] = None,
        public_key: Optional[str] = None,
        private_key: Optional[str] = None,
        passphrase_secret: Optional[str] = None
    ) -> None:
        self.onelake_fs = fsspec.filesystem(
            file_system_code,
            account_name="onelake",
            account_host="onelake.dfs.fabric.microsoft.com",
            client_id=os.getenv("AZURE_CLIENT_ID"),
            client_secret=os.getenv("AZURE_CLIENT_SECRET"),
            tenant_id=os.getenv("AZURE_TENANT_ID")
        )
        
        if not public_key_secret and not public_key:
            raise ValueError("Either public_key_secret or public_key must be provided.")
        if not private_key_secret and not private_key:
            raise ValueError("Either private_key_secret or private_key must be provided.")
            
        self.public_key = (
            get_secret_notebookutils(public_key_secret, key_vault_name)
            if public_key_secret
            else public_key
        )
        self.private_key = (
            get_secret_notebookutils(private_key_secret, key_vault_name)
            if private_key_secret
            else private_key
        )
        
        try:
            self.pubkey, _ = pgpy.PGPKey.from_blob(self.public_key)
            self.privkey, _ = pgpy.PGPKey.from_blob(self.private_key)
        except Exception as e:
            raise ValueError(f"Failed to load PGP keys: {str(e)}")

        # Validate public key User ID
        if not self.pubkey.userids:
            try:
                uid = pgpy.PGPUID.new("Temporary User")
                self.pubkey.add_uid(
                    uid,
                    usage={pgpy.constants.KeyFlags.EncryptCommunications, pgpy.constants.KeyFlags.EncryptStorage},
                    hashes=[pgpy.constants.HashAlgorithm.SHA256],
                    ciphers=[pgpy.constants.SymmetricKeyAlgorithm.AES256]
                )
            except Exception as e:
                raise ValueError(f"Public key lacks User ID and cannot be modified: {str(e)}")

        self.passphrase = None
        if passphrase_secret:
            try:
                self.passphrase = get_secret_notebookutils(passphrase_secret, key_vault_name).strip()
                # Debug passphrase
                print(f"Passphrase length: {len(self.passphrase)}")
                print(f"Passphrase start: {self.passphrase[:5]}")
                print(f"Private key start: {self.private_key[:50]}")
            except Exception as e:
                raise ValueError(f"Failed to load passphrase from key vault: {str(e)}")
        
        if self.privkey.is_protected:
            if not self.passphrase:
                raise ValueError("Passphrase secret is required for protected private key.")
            try:
                with self.privkey.unlock(self.passphrase):
                    pass
            except pgpy.errors.PGPError as e:
                raise ValueError(f"Invalid passphrase for private key: {str(e)}")

    def encrypt_file(self, input_file: str, output_path: str) -> 'PGP':
        if not self.onelake_fs.exists(input_file):
            raise FileNotFoundError(f"Input file not found: {input_file}")
            
        try:
            chunks = []
            with self.onelake_fs.open(input_file, "rb") as rb_file:
                while True:
                    chunk = rb_file.read(8192)
                    if not chunk:
                        break
                    chunks.append(chunk)
            file_content = b"".join(chunks)
            file_message = pgpy.PGPMessage.new(file_content, file=True)
            encrypted_message = self.pubkey.encrypt(file_message)
            
            file_name = os.path.basename(input_file) + ".pgp"
            encrypted_path = os.path.join(output_path, file_name)
            
            with self.onelake_fs.open(encrypted_path, "w") as w_file:
                w_file.write(str(encrypted_message))
                
            return self
        except Exception as e:
            raise IOError(f"Failed to encrypt file: {str(e)}")
        
    def decrypt_file(self, input_file: str, output_path: str) -> 'PGP':
        if not self.onelake_fs.exists(input_file):
            raise FileNotFoundError(f"Input file not found: {input_file}")
            
        try:
            chunks = []
            with self.onelake_fs.open(input_file, "rb") as rb_file:
                while True:
                    chunk = rb_file.read(8192)
                    if not chunk:
                        break
                    chunks.append(chunk)
            encrypted_content = b"".join(chunks)
            
            enc_message = pgpy.PGPMessage.from_blob(encrypted_content)
            decrypted_message = (
                self.privkey.unlock(self.passphrase).__enter__().decrypt(enc_message).message
                if self.privkey.is_protected
                else self.privkey.decrypt(enc_message).message
            )
            
            file_name = os.path.basename(input_file).removesuffix(".pgp")
            decrypted_path = os.path.join(output_path, file_name)
            
            mode = "wb" if isinstance(decrypted_message, (bytes, bytearray)) else "w"
            with self.onelake_fs.open(decrypted_path, mode) as w_file:
                w_file.write(decrypted_message)
                
            return self
        except Exception as e:
            raise IOError(f"Failed to decrypt file: {str(e)}")
