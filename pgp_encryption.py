import pgpy
import fsspec
from spark_engine.common.email_util import get_secret_notebookutils

class PGP:
    def __init__(
        self,
        key_vault_name: str = None,
        public_key_secret: str = None,
        private_key_secret: str = None,
        private_key: str = None,
        passphrase_secret: str = None
    ):
        """
        Initialize PGP class for encryption and decryption.

        Args:
            key_vault_name: Name of the Azure Key Vault.
            public_key_secret: Secret name for the public key in the key vault.
            private_key_secret: Secret name for the private key in the key vault (optional if private_key is provided).
            private_key: Private key as a string (e.g., from keyring).
            passphrase_secret: Secret name for the passphrase in the key vault (optional).

        Raises:
            ValueError: If required parameters are missing or keys cannot be loaded.
        """
        self.key_vault_name = key_vault_name
        self.public_key_secret = public_key_secret
        self.private_key_secret = private_key_secret
        self.passphrase_secret = passphrase_secret
        self.public_key = None
        self.private_key = None
        self.passphrase = None

        # Load public key from key vault
        if key_vault_name and public_key_secret:
            try:
                public_key_str = get_secret_notebookutils(public_key_secret, key_vault_name)
                self.public_key, _ = pgpy.PGPKey.from_blob(public_key_str)
            except Exception as e:
                raise ValueError(f"Failed to load public key from key vault: {str(e)}")

        # Load private key from keyring or key vault
        if private_key:
            try:
                self.private_key, _ = pgpy.PGPKey.from_blob(private_key)
            except Exception as e:
                raise ValueError(f"Failed to load private key from provided string: {str(e)}")
        elif key_vault_name and private_key_secret:
            try:
                private_key_str = get_secret_notebookutils(private_key_secret, key_vault_name)
                self.private_key, _ = pgpy.PGPKey.from_blob(private_key_str)
            except Exception as e:
                raise ValueError(f"Failed to load private key from key vault: {str(e)}")

        # Load passphrase from key vault
        if key_vault_name and passphrase_secret:
            try:
                self.passphrase = get_secret_notebookutils(passphrase_secret, key_vault_name)
            except Exception as e:
                raise ValueError(f"Failed to load passphrase from key vault: {str(e)}")

    def decrypt_file(self, input_file: str, output_path: str, passphrase: str = None):
        """
        Decrypt a file using the private key.

        Args:
            input_file: Path to the encrypted file (e.g., .pgp).
            output_path: Directory path for the decrypted output file.
            passphrase: Passphrase to unlock the private key (optional if loaded from key vault).

        Raises:
            FileNotFoundError: If the input file does not exist.
            IOError: If decryption or file operations fail.
            ValueError: If private key or passphrase is missing.
        """
        if not self.private_key:
            raise ValueError("Private key is not loaded")

        # Use passphrase from key vault if available, otherwise use provided passphrase
        effective_passphrase = self.passphrase if self.passphrase else passphrase
        if self.private_key.is_protected and not effective_passphrase:
            raise ValueError("Passphrase is required for protected private key")

        # Initialize fsspec filesystem
        fs = fsspec.filesystem("abfss", account_name="onelake", account_host="onelake.dfs.fabric.microsoft.com")

        # Check if input file exists
        if not fs.exists(input_file):
            raise FileNotFoundError(f"Input file not found: {input_file}")

        # Read encrypted file
        try:
            with fs.open(input_file, "rb") as f:
                encrypted_message = pgpy.PGPMessage.from_blob(f.read())
        except Exception as e:
            raise IOError(f"Failed to read encrypted file {input_file}: {str(e)}")

        # Decrypt the message
        try:
            if self.private_key.is_protected:
                with self.private_key.unlock(effective_passphrase):
                    decrypted_message = self.private_key.decrypt(encrypted_message).message
            else:
                decrypted_message = self.private_key.decrypt(encrypted_message).message
        except Exception as e:
            raise IOError(f"Failed to decrypt file {input_file}: {str(e)}")

        # Write decrypted file
        output_file = os.path.join(output_path, os.path.basename(input_file).replace(".pgp", ""))
        try:
            with fs.open(output_file, "wb") as f:
                f.write(decrypted_message)
        except Exception as e:
            raise IOError(f"Failed to write decrypted file {output_file}: {str(e)}")
