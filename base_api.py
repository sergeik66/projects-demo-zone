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
        self.key_vault_name = key_vault_name
        self.public_key_secret = public_key_secret
        self.private_key_secret = private_key_secret
        self.passphrase_secret = passphrase_secret
        self.public_key = public_key
        self.private_key = private_key
        
        # Load public key for encryption
        if not public_key_secret and not public_key:
            raise ValueError("Either public_key_secret or public_key must be provided.")
        public_key_str = (
            get_secret_notebookutils(public_key_secret, key_vault_name)
            if public_key_secret
            else public_key
        )
        try:
            self.pubkey, _ = pgpy.PGPKey.from_blob(public_key_str)
            if not self.pubkey.userids:
                uid = pgpy.PGPUID.new("Anonymous")
                self.pubkey.add_uid(
                    uid,
                    usage={pgpy.constants.KeyFlags.EncryptCommunications, pgpy.constants.KeyFlags.EncryptStorage},
                    hashes=[pgpy.constants.HashAlgorithm.SHA256],
                    ciphers=[pgpy.constants.SymmetricKeyAlgorithm.AES256]
                )
        except Exception as e:
            raise ValueError(f"Failed to load public key: {str(e)}")

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
            # Load private key and passphrase fresh, as in standalone test
            if not self.private_key_secret and not self.private_key:
                raise ValueError("Either private_key_secret or private_key must be provided.")
            private_key_str = (
                get_secret_notebookutils(self.private_key_secret, self.key_vault_name)
                if self.private_key_secret
                else self.private_key
            )
            passphrase = (
                get_secret_notebookutils(self.passphrase_secret, self.key_vault_name)
                if self.passphrase_secret
                else None
            )
            privkey, _ = pgpy.PGPKey.from_blob(private_key_str)
            
            # Debug
            print(f"Passphrase length: {len(passphrase) if passphrase else 0}")
            print(f"Passphrase start: {passphrase[:5] if passphrase else 'None'}...")
            print(f"Private key start: {private_key_str[:50]}...")
            print(f"Private key protected: {privkey.is_protected}")
            print(f"Private key fingerprint: {privkey.fingerprint}")
            
            chunks = []
            with self.onelake_fs.open(input_file, "rb") as rb_file:
                while True:
                    chunk = rb_file.read(8192)
                    if not chunk:
                        break
                    chunks.append(chunk)
            encrypted_content = b"".join(chunks)
            
            enc_message = pgpy.PGPMessage.from_blob(encrypted_content)
            if privkey.is_protected and not passphrase:
                raise ValueError("Passphrase is required for protected private key.")
            with privkey.unlock(passphrase):
                decrypted_message = privkey.decrypt(enc_message).message
            
            file_name = os.path.basename(input_file).removesuffix(".pgp")
            decrypted_path = os.path.join(output_path, file_name)
            
            mode = "wb" if isinstance(decrypted_message, (bytes, bytearray)) else "w"
            with self.onelake_fs.open(decrypted_path, mode) as w_file:
                w_file.write(decrypted_message)
                
            return self
        except Exception as e:
            raise IOError(f"Failed to decrypt file: {str(e)}")
