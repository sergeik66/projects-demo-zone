import unittest
from unittest.mock import patch, Mock, mock_open
from spark_engine.common.pgp import PGP
import pgpy
import fsspec
import os

class TestPGP(unittest.TestCase):
    def setUp(self):
        """Set up test fixtures and mocks."""
        self.key_vault_name = "test-vault"
        self.public_key_secret = "mock-public-key-secret"
        self.private_key_secret = "mock-private-key-secret"
        self.passphrase_secret = "mock-passphrase-secret"
        self.public_key_str = "-----BEGIN PGP PUBLIC KEY BLOCK-----\nmock-public-key\n-----END PGP PUBLIC KEY BLOCK-----"
        self.private_key_str = "-----BEGIN PGP PRIVATE KEY BLOCK-----\nmock-private-key\n-----END PGP PRIVATE KEY BLOCK-----"
        self.passphrase = "my-passphrase"
        self.input_file = "abfss://test/decrypted/test.txt"
        self.output_path = "abfss://test/encrypted"
        self.encrypted_file = "abfss://test/encrypted/test.txt.pgp"
        
        # Mock fsspec filesystem
        self.fs_patcher = patch("spark_engine.common.pgp.fsspec.filesystem")
        self.mock_fs = self.fs_patcher.start()
        self.mock_fs_instance = Mock()
        self.mock_fs.return_value = self.mock_fs_instance
        self.mock_fs_instance.exists.return_value = True
        self.mock_fs_instance.ls.return_value = [self.input_file]
        self.mock_fs_instance.open = mock_open(read_data=b"test content")
        
        # Mock get_secret_notebookutils
        self.secret_patcher = patch("spark_engine.common.pgp.get_secret_notebookutils")
        self.mock_get_secret = self.secret_patcher.start()
        self.mock_get_secret.side_effect = [self.public_key_str, self.private_key_str, self.passphrase]
        
        # Mock pgpy.PGPKey
        self.key_patcher = patch("spark_engine.common.pgp.pgpy.PGPKey")
        self.mock_pgp_key = self.key_patcher.start()
        self.mock_pubkey = Mock()
        self.mock_privkey = Mock(is_protected=True)
        self.mock_privkey.unlock = Mock(return_value=Mock(__enter__=Mock(return_value=self.mock_privkey)))
        self.mock_pgp_key.from_blob.side_effect = [
            (self.mock_pubkey, None),
            (self.mock_privkey, None)
        ]
        
        # Mock pgpy.PGPMessage
        self.message_patcher = patch("spark_engine.common.pgp.pgpy.PGPMessage")
        self.mock_pgp_message = self.message_patcher.start()
        self.mock_message = Mock()
        self.mock_encrypted_message = Mock()
        self.mock_message.__str__.return_value = "encrypted content"
        self.mock_pgp_message.new.return_value = self.mock_message
        self.mock_pgp_message.from_blob.return_value = self.mock_encrypted_message
        self.mock_privkey.decrypt.return_value.message = b"decrypted content"
        self.mock_pubkey.encrypt.return_value = self.mock_encrypted_message

    def tearDown(self):
        """Clean up patchers."""
        self.fs_patcher.stop()
        self.secret_patcher.stop()
        self.key_patcher.stop()
        self.message_patcher.stop()

    def test_init_with_key_vault_secrets_success(self):
        """Test successful initialization with Key Vault secrets."""
        pgp = PGP(
            key_vault_name=self.key_vault_name,
            public_key_secret=self.public_key_secret,
            private_key_secret=self.private_key_secret,
            passphrase_secret=self.passphrase_secret
        )
        self.assertEqual(pgp.public_key, self.public_key_str)
        self.assertEqual(pgp.private_key, self.private_key_str)
        self.assertEqual(pgp.passphrase, self.passphrase)
        self.mock_get_secret.assert_any_call(self.public_key_secret, self.key_vault_name)
        self.mock_get_secret.assert_any_call(self.private_key_secret, self.key_vault_name)
        self.mock_get_secret.assert_any_call(self.passphrase_secret, self.key_vault_name)
        self.mock_pgp_key.from_blob.assert_called()

    def test_init_with_key_strings_success(self):
        """Test successful initialization with direct key strings."""
        pgp = PGP(
            key_vault_name=self.key_vault_name,
            public_key=self.public_key_str,
            private_key=self.private_key_str,
            passphrase_secret=self.passphrase_secret
        )
        self.assertEqual(pgp.public_key, self.public_key_str)
        self.assertEqual(pgp.private_key, self.private_key_str)
        self.assertEqual(pgp.passphrase, self.passphrase)
        self.mock_get_secret.assert_called_once_with(self.passphrase_secret, self.key_vault_name)

    def test_init_missing_public_key_raises_value_error(self):
        """Test initialization fails without public key or secret."""
        with self.assertRaisesRegex(ValueError, "Either public_key_secret or public_key must be provided"):
            PGP(key_vault_name=self.key_vault_name, private_key_secret=self.private_key_secret)

    def test_init_missing_private_key_raises_value_error(self):
        """Test initialization fails without private key or secret."""
        with self.assertRaisesRegex(ValueError, "Either private_key_secret or private_key must be provided"):
            PGP(key_vault_name=self.key_vault_name, public_key_secret=self.public_key_secret)

    def test_init_missing_passphrase_for_protected_key_raises_value_error(self):
        """Test initialization fails if passphrase is missing for protected private key."""
        with self.assertRaisesRegex(ValueError, "Passphrase secret is required for protected private key"):
            PGP(
                key_vault_name=self.key_vault_name,
                public_key_secret=self.public_key_secret,
                private_key_secret=self.private_key_secret
            )

    def test_init_invalid_key_blob_raises_value_error(self):
        """Test initialization fails with invalid key blob."""
        self.mock_pgp_key.from_blob.side_effect = Exception("Invalid key format")
        with self.assertRaisesRegex(ValueError, "Failed to load PGP keys"):
            PGP(
                key_vault_name=self.key_vault_name,
                public_key_secret=self.public_key_secret,
                private_key_secret=self.private_key_secret,
                passphrase_secret=self.passphrase_secret
            )

    def test_encrypt_file_success(self):
        """Test successful file encryption."""
        pgp = PGP(
            key_vault_name=self.key_vault_name,
            public_key_secret=self.public_key_secret,
            private_key_secret=self.private_key_secret,
            passphrase_secret=self.passphrase_secret
        )
        result = pgp.encrypt_file(self.input_file, self.output_path)
        self.assertIs(result, pgp)
        self.mock_fs_instance.exists.assert_called_with(self.input_file)
        self.mock_pgp_message.new.assert_called()
        self.mock_pubkey.encrypt.assert_called()
        self.mock_fs_instance.open.assert_called_with(os.path.join(self.output_path, "test.txt.pgp"), "w")

    def test_encrypt_file_not_found_raises_file_not_found_error(self):
        """Test encryption fails if input file does not exist."""
        self.mock_fs_instance.exists.return_value = False
        pgp = PGP(
            key_vault_name=self.key_vault_name,
            public_key_secret=self.public_key_secret,
            private_key_secret=self.private_key_secret,
            passphrase_secret=self.passphrase_secret
        )
        with self.assertRaises(FileNotFoundError):
            pgp.encrypt_file(self.input_file, self.output_path)

    def test_encrypt_file_io_error_raises_io_error(self):
        """Test encryption fails with IO error."""
        self.mock_fs_instance.open.side_effect = Exception("IO error")
        pgp = PGP(
            key_vault_name=self.key_vault_name,
            public_key_secret=self.public_key_secret,
            private_key_secret=self.private_key_secret,
            passphrase_secret=self.passphrase_secret
        )
        with self.assertRaisesRegex(IOError, "Failed to encrypt file"):
            pgp.encrypt_file(self.input_file, self.output_path)

    def test_decrypt_file_success(self):
        """Test successful file decryption with protected private key."""
        pgp = PGP(
            key_vault_name=self.key_vault_name,
            public_key_secret=self.public_key_secret,
            private_key_secret=self.private_key_secret,
            passphrase_secret=self.passphrase_secret
        )
        result = pgp.decrypt_file(self.encrypted_file, self.output_path)
        self.assertIs(result, pgp)
        self.mock_fs_instance.exists.assert_called_with(self.encrypted_file)
        self.mock_pgp_message.from_blob.assert_called()
        self.mock_privkey.unlock.assert_called_with(self.passphrase)
        self.mock_privkey.decrypt.assert_called()
        self.mock_fs_instance.open.assert_called_with(os.path.join(self.output_path, "test.txt"), "wb")

    def test_decrypt_file_not_found_raises_file_not_found_error(self):
        """Test decryption fails if input file does not exist."""
        self.mock_fs_instance.exists.return_value = False
        pgp = PGP(
            key_vault_name=self.key_vault_name,
            public_key_secret=self.public_key_secret,
            private_key_secret=self.private_key_secret,
            passphrase_secret=self.passphrase_secret
        )
        with self.assertRaises(FileNotFoundError):
            pgp.decrypt_file(self.encrypted_file, self.output_path)

    def test_decrypt_file_io_error_raises_io_error(self):
        """Test decryption fails with IO error."""
        self.mock_fs_instance.open.side_effect = Exception("IO error")
        pgp = PGP(
            key_vault_name=self.key_vault_name,
            public_key_secret=self.public_key_secret,
            private_key_secret=self.private_key_secret,
            passphrase_secret=self.passphrase_secret
        )
        with self.assertRaisesRegex(IOError, "Failed to decrypt file"):
            pgp.decrypt_file(self.encrypted_file, self.output_path)

    def test_decrypt_file_invalid_passphrase_raises_io_error(self):
        """Test decryption fails with invalid passphrase."""
        self.mock_privkey.unlock.side_effect = pgpy.errors.PGPError("Invalid passphrase")
        pgp = PGP(
            key_vault_name=self.key_vault_name,
            public_key_secret=self.public_key_secret,
            private_key_secret=self.private_key_secret,
            passphrase_secret=self.passphrase_secret
        )
        with self.assertRaisesRegex(IOError, "Failed to decrypt file"):
            pgp.decrypt_file(self.encrypted_file, self.output_path)

if __name__ == "__main__":
    unittest.main()
