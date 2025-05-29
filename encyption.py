import fsspec
import pgpy
import os
from typing import Optional, Union
from spark_engine.common.email_util import get_secret_notebookutils

class PGP:
    """A class to handle PGP encryption and decryption of files using fsspec and pgpy."""

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
        """
        Initialize the PGP class with key vault and file system configurations.
        
        Args:
            key_vault_name: Name of the key vault for retrieving secrets.
            file_system_code: File system protocol (default: "abfss").
            public_key_secret: Secret name for public key (optional).
            private_key_secret: Secret name for private key (optional).
            public_key: Public key string (optional, used if public_key_secret is None).
            private_key: Private key string (optional, used if private_key_secret is None).
            passphrase_secret: Secret name for the passphrase in the key vault (optional).
        
        Raises:
            ValueError: If neither secret nor key is provided for public/private key,
                        or if passphrase_secret is missing for a protected private key.
        """
        self.onelake_fs = fsspec.filesystem(
            file_system_code,
            account_name="onelake",
            account_host="onelake.dfs.fabric.microsoft.com"
        )
        
        # Validate and load keys
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

        # Load passphrase from key vault if provided
        self.passphrase = None
        if passphrase_secret:
            try:
                self.passphrase = get_secret_notebookutils(passphrase_secret, key_vault_name)
            except Exception as e:
                raise ValueError(f"Failed to load passphrase from key vault: {str(e)}")
        
        # Validate passphrase for protected private key
        if self.privkey.is_protected and not self.passphrase:
            raise ValueError("Passphrase secret is required for protected private key.")

    def encrypt_file(self, input_file: str, output_path: str) -> 'PGP':
        """
        Encrypt a file using the public key and save it to the output path.
        
        Args:
            input_file: Path to the input file.
            output_path: Directory path for the encrypted output file.
        
        Returns:
            Self for method chaining.
        
        Raises:
            FileNotFoundError: If input file does not exist.
            IOError: If file operations fail.
        """
        if not self.onelake_fs.exists(input_file):
            raise FileNotFoundError(f"Input file not found: {input_file}")
            
        try:
            with self.onelake_fs.open(input_file, "rb") as rb_file:
                file_message = pgpy.PGPMessage.new(rb_file.read(), file=True)
            encrypted_message = self.pubkey.encrypt(file_message)
            
            file_name = os.path.basename(input_file) + ".pgp"
            encrypted_path = os.path.join(output_path, file_name)
            
            with self.onelake_fs.open(encrypted_path, "w") as w_file:
                w_file.write(str(encrypted_message))
                
            return self
        except Exception as e:
            raise IOError(f"Failed to encrypt file: {str(e)}")
        
    def decrypt_file(self, input_file: str, output_path: str) -> 'PGP':
        """
        Decrypt a file using the private key and save it to the output path.
        
        Args:
            input_file: Path to the encrypted input file.
            output_path: Directory path for the decrypted output file.
        
        Returns:
            Self for method chaining.
        
        Raises:
            FileNotFoundError: If input file does not exist.
            ValueError: If passphrase is required but not provided.
            IOError: If file operations or decryption fail.
        """
        if not self.onelake_fs.exists(input_file):
            raise FileNotFoundError(f"Input file not found: {input_file}")
            
        try:
            with self.onelake_fs
