#!/usr/bin/env python3

import argparse
import logging
import os
import sys
from typing import Optional

import pgpy

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)

class PGP:
    """A class to handle PGP encryption and decryption of files."""
    
    def __init__(
        self,
        public_key_path: Optional[str] = None,
        private_key_path: Optional[str] = None,
        passphrase: Optional[str] = None
    ) -> None:
        """
        Initialize the PGP class with key paths and passphrase.

        Args:
            public_key_path: Path to the public key file.
            private_key_path: Path to the private key file.
            passphrase: Passphrase for the private key.

        Raises:
            ValueError: If required keys are missing or invalid.
        """
        self.public_key = None
        self.passphrase = passphrase

        if public_key_path:
            try:
                self.public_key, _ = pgpy.PGPKey.from_file(public_key_path)
                if not self.public_key.userids:
                    logger.debug("Adding default User ID to public key")
                    uid = pgpy.PGPUID.new("Anonymous")
                    self.public_key.add_uid(
                        uid,
                        usage={pgpy.constants.KeyFlags.EncryptCommunications, pgpy.constants.KeyFlags.EncryptStorage},
                        hashes=[pgpy.constants.HashAlgorithm.SHA256],
                        ciphers=[pgpy.constants.SymmetricKeyAlgorithm.AES256]
                    )
            except Exception as e:
                raise ValueError(f"Failed to load public key from {public_key_path}: {str(e)}")

        self.private_key_path = private_key_path  # Store path for on-demand loading

    def encrypt_file(self, input_file: str, output_path: str) -> None:
        """
        Encrypt a file using the public key.

        Args:
            input_file: Path to the input file.
            output_path: Directory path for the encrypted output (.pgp).

        Raises:
            FileNotFoundError: If input file or output path is invalid.
            ValueError: If public key is not provided.
            IOError: If encryption or file operations fail.
        """
        if not self.public_key:
            raise ValueError("Public key is required for encryption")
        if not os.path.isfile(input_file):
            raise FileNotFoundError(f"Input file not found: {input_file}")
        if not os.path.isdir(output_path):
            raise FileNotFoundError(f"Output directory not found: {output_path}")

        try:
            # Read input file in chunks
            chunks = []
            with open(input_file, "rb") as f:
                while True:
                    chunk = f.read(8192)
                    if not chunk:
                        break
                    chunks.append(chunk)
            file_content = b"".join(chunks)

            # Encrypt
            message = pgpy.PGPMessage.new(file_content, file=True)
            encrypted_message = self.public_key.encrypt(message)

            # Save encrypted file
            output_file = os.path.join(output_path, f"{os.path.basename(input_file)}.pgp")
            with open(output_file, "w") as f:
                f.write(str(encrypted_message))
            logger.info(f"Encrypted {input_file} to {output_file}")
        except Exception as e:
            raise IOError(f"Failed to encrypt {input_file}: {str(e)}")

    def decrypt_file(self, input_file: str, output_path: str, delete_encrypted: bool = False) -> None:
        """
        Decrypt a .pgp file using the private key and passphrase.

        Args:
            input_file: Path to the encrypted .pgp file.
            output_path: Directory path for the decrypted output.
            delete_encrypted: Whether to delete the encrypted file after decryption.

        Raises:
            FileNotFoundError: If input file or output path is invalid.
            ValueError: If private key or passphrase is missing.
            IOError: If decryption or file operations fail.
        """
        if not self.private_key_path:
            raise ValueError("Private key path is required for decryption")
        if not os.path.isfile(input_file):
            raise FileNotFoundError(f"Input file not found: {input_file}")
        if not os.path.isdir(output_path):
            raise FileNotFoundError(f"Output directory not found: {output_path}")
        if not input_file.endswith(".pgp"):
            raise ValueError(f"Input file must have .pgp extension: {input_file}")

        try:
            # Load private key fresh to avoid state issues
            private_key, _ = pgpy.PGPKey.from_file(self.private_key_path)
            logger.debug(f"Private key protected: {private_key.is_protected}")
            logger.debug(f"Private key fingerprint: {private_key.fingerprint}")
            if private_key.is_protected and not self.passphrase:
                raise ValueError("Passphrase is required for protected private key")
            if self.passphrase:
                logger.debug(f"Passphrase length: {len(self.passphrase)}")
                logger.debug(f"Passphrase start: {self.passphrase[:5]}...")

            # Read encrypted file
            chunks = []
            with open(input_file, "rb") as f:
                while True:
                    chunk = f.read(8192)
                    if not chunk:
                        break
                    chunks.append(chunk)
            encrypted_content = b"".join(chunks)

            # Decrypt
            enc_message = pgpy.PGPMessage.from_blob(encrypted_content)
            with private_key.unlock(self.passphrase):
                decrypted_message = private_key.decrypt(enc_message).message

            # Save decrypted file
            output_file = os.path.join(output_path, os.path.basename(input_file).removesuffix(".pgp"))
            mode = "wb" if isinstance(decrypted_message, (bytes, bytearray)) else "w"
            with open(output_file, mode) as f:
                f.write(decrypted_message)
            logger.info(f"Decrypted {input_file} to {output_file}")

            # Delete encrypted file if requested
            if delete_encrypted and os.path.exists(input_file):
                os.remove(input_file)
                logger.info(f"Deleted encrypted file: {input_file}")

            # Verify decrypted content
            with open(output_file, "r") as f:
                preview = f.read(100)
                logger.info(f"Decrypted content preview: {preview}...")
        except Exception as e:
            raise IOError(f"Failed to decrypt {input_file}: {str(e)}")

def main():
    """Command-line interface for PGP encryption and decryption."""
    parser = argparse.ArgumentParser(description="Encrypt or decrypt files using PGP.")
    parser.add_argument("action", choices=["encrypt", "decrypt"], help="Action to perform: encrypt or decrypt")
    parser.add_argument("--input-file", required=True, help="Path to the input file")
    parser.add_argument("--output-dir", required=True, help="Directory for output files")
    parser.add_argument("--public-key", help="Path to public key file (required for encryption)")
    parser.add_argument("--private-key", help="Path to private key file (required for decryption)")
    parser.add_argument("--passphrase", help="Passphrase for private key (required for protected keys)")
    parser.add_argument("--delete-encrypted", action="store_true", help="Delete encrypted file after decryption")
    parser.add_argument("--verbose", action="store_true", help="Enable debug logging")

    args = parser.parse_args()

    # Set logging level
    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)

    try:
        pgp = PGP(
            public_key_path=args.public_key,
            private_key_path=args.private_key,
            passphrase=args.passphrase
        )

        if args.action == "encrypt":
            pgp.encrypt_file(args.input_file, args.output_dir)
        elif args.action == "decrypt":
            pgp.decrypt_file(args.input_file, args.output_dir, delete_encrypted=args.delete_encrypted)

    except Exception as e:
        logger.error(str(e))
        sys.exit(1)

if __name__ == "__main__":
    main()
