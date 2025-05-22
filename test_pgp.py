import pytest
import pgpy
import os
from unittest.mock import Mock, patch, mock_open
from pgp import PGP  # Adjust import based on your module structure

# Mock PGP keys for testing
MOCK_PUBLIC_KEY = "mock_public_key_data"
MOCK_PRIVATE_KEY = "mock_private_key_data"
MOCK_PASSPHRASE = "test_passphrase"
MOCK_FILE_CONTENT = b"test content"
MOCK_ENCRYPTED_CONTENT = "encrypted_content"
MOCK_DECRYPTED_CONTENT = b"decrypted content"

@pytest.fixture
def mock_filesystem():
    """Fixture to mock fsspec filesystem."""
    fs = Mock()
    fs.exists.return_value = True
    fs.open = mock_open()
    return fs

@pytest.fixture
def mock_pgp_key():
    """Fixture to mock PGPKey and PGPMessage."""
    key = Mock()
    key.is_protected = False
    key.encrypt.return_value = MOCK_ENCRYPTED_CONTENT
    key.decrypt.return_value = Mock(message=MOCK_DECRYPTED_CONTENT)
    return key, None

@pytest.fixture
def mock_protected_pgp_key():
    """Fixture to mock a protected PGPKey."""
    key = Mock()
    key.is_protected = True
    key.unlock.return_value.__enter__.return_value = key
    key.decrypt.return_value = Mock(message=MOCK_DECRYPTED_CONTENT)
    return key, None

@pytest.fixture
def pgp_instance(mock_filesystem):
    """Fixture to create a PGP instance with mocked dependencies."""
    with patch("pgp.fsspec.filesystem", return_value=mock_filesystem), \
         patch("pgp.get_secret_notebookutils", side_effect=[MOCK_PUBLIC_KEY, MOCK_PRIVATE_KEY]), \
         patch("pgp.pgpy.PGPKey.from_blob", side_effect=[(Mock(), None), (Mock(), None)]):
        pgp = PGP(
            key_vault_name="test_vault",
            public_key_secret="public_secret",
            private_key_secret="private_secret"
        )
        return pgp

def test_pgp_init_with_secrets(mock_filesystem):
    """Test PGP initialization with secret keys."""
    with patch("pgp.get_secret_notebookutils", side_effect=[MOCK_PUBLIC_KEY, MOCK_PRIVATE_KEY]), \
         patch("pgp.pgpy.PGPKey.from_blob", side_effect=[(Mock(), None), (Mock(), None)]):
        pgp = PGP(
            key_vault_name="test_vault",
            public_key_secret="public_secret",
            private_key_secret="private_secret"
        )
    assert pgp.public_key == MOCK_PUBLIC_KEY
    assert pgp.private_key == MOCK_PRIVATE_KEY
    assert pgp.onelake_fs == mock_filesystem

def test_pgp_init_with_direct_keys(mock_filesystem):
    """Test PGP initialization with direct keys."""
    with patch("pgp.pgpy.PGPKey.from_blob", side_effect=[(Mock(), None), (Mock(), None)]):
        pgp = PGP(
            key_vault_name="test_vault",
            public_key=MOCK_PUBLIC_KEY,
            private_key=MOCK_PRIVATE_KEY
        )
    assert pgp.public_key == MOCK_PUBLIC_KEY
    assert pgp.private_key == MOCK_PRIVATE_KEY

def test_pgp_init_missing_keys():
    """Test PGP initialization raises error if no keys provided."""
    with pytest.raises(ValueError, match="Either public_key_secret or public_key must be provided."):
        PGP(key_vault_name="test_vault", private_key=MOCK_PRIVATE_KEY)
    with pytest.raises(ValueError, match="Either private_key_secret or private_key must be provided."):
        PGP(key_vault_name="test_vault", public_key=MOCK_PUBLIC_KEY)

def test_pgp_init_invalid_key(mock_filesystem):
    """Test PGP initialization raises error for invalid keys."""
    with patch("pgp.get_secret_notebookutils", side_effect=[MOCK_PUBLIC_KEY, MOCK_PRIVATE_KEY]), \
         patch("pgp.pgpy.PGPKey.from_blob", side_effect=ValueError("Invalid key")):
        with pytest.raises(ValueError, match="Failed to load PGP keys: Invalid key"):
            PGP(
                key_vault_name="test_vault",
                public_key_secret="public_secret",
                private_key_secret="private_secret"
            )

def test_encrypt_file_success(pgp_instance, mock_filesystem, mock_pgp_key):
    """Test successful file encryption."""
    with patch("pgp.pgpy.PGPKey.from_blob", return_value=mock_pgp_key), \
         patch("pgp.pgpy.PGPMessage.new", return_value=Mock()):
        pgp_instance.pubkey = mock_pgp_key[0]
        result = pgp_instance.encrypt_file(
            input_file="abfss://test/input.txt",
            output_path="abfss://test/output"
        )
    
    assert result == pgp_instance  # Method chaining
    mock_filesystem.exists.assert_called_with("abfss://test/input.txt")
    mock_filesystem.open.assert_any_call("abfss://test/input.txt", "rb")
    mock_filesystem.open.assert_any_call("abfss://test/output/input.txt.pgp", "w")
    pgp_instance.pubkey.encrypt.assert_called_once()

def test_encrypt_file_not_found(pgp_instance, mock_filesystem):
    """Test encryption raises FileNotFoundError for missing file."""
    mock_filesystem.exists.return_value = False
    with pytest.raises(FileNotFoundError, match="Input file not found: abfss://test/input.txt"):
        pgp_instance.encrypt_file(
            input_file="abfss://test/input.txt",
            output_path="abfss://test/output"
        )

def test_encrypt_file_io_error(pgp_instance, mock_filesystem, mock_pgp_key):
    """Test encryption raises IOError for file operation failure."""
    with patch("pgp.pgpy.PGPKey.from_blob", return_value=mock_pgp_key), \
         patch("pgp.pgpy.PGPMessage.new", side_effect=IOError("File error")):
        pgp_instance.pubkey = mock_pgp_key[0]
        with pytest.raises(IOError, match="Failed to encrypt file: File error"):
            pgp_instance.encrypt_file(
                input_file="abfss://test/input.txt",
                output_path="abfss://test/output"
            )

def test_decrypt_file_success(pgp_instance, mock_filesystem, mock_pgp_key):
    """Test successful file decryption."""
    with patch("pgp.pgpy.PGPKey.from_blob", return_value=mock_pgp_key), \
         patch("pgp.pgpy.PGPMessage.from_blob", return_value=Mock()):
        pgp_instance.privkey = mock_pgp_key[0]
        result = pgp_instance.decrypt_file(
            input_file="abfss://test/input.txt.pgp",
            output_path="abfss://test/output"
        )
    
    assert result == pgp_instance  # Method chaining
    mock_filesystem.exists.assert_called_with("abfss://test/input.txt.pgp")
    mock_filesystem.open.assert_any_call("abfss://test/input.txt.pgp", "rb")
    mock_filesystem.open.assert_any_call("abfss://test/output/input.txt", "wb")
    pgp_instance.privkey.decrypt.assert_called_once()

def test_decrypt_file_protected_key(pgp_instance, mock_filesystem, mock_protected_pgp_key):
    """Test decryption with protected private key."""
    with patch("pgp.pgpy.PGPKey.from_blob", return_value=mock_protected_pgp_key), \
         patch("pgp.pgpy.PGPMessage.from_blob", return_value=Mock()):
        pgp_instance.privkey = mock_protected_pgp_key[0]
        result = pgp_instance.decrypt_file(
            input_file="abfss://test/input.txt.pgp",
            output_path="abfss://test/output",
            passphrase=MOCK_PASSPHRASE
        )
    
    assert result == pgp_instance
    pgp_instance.privkey.unlock.assert_called_with(MOCK_PASSPHRASE)
    pgp_instance.privkey.decrypt.assert_called_once()

def test_decrypt_file_missing_passphrase(pgp_instance, mock_filesystem, mock_protected_pgp_key):
    """Test decryption raises error for missing passphrase."""
    with patch("pgp.pgpy.PGPKey.from_blob", return_value=mock_protected_pgp_key), \
         patch("pgp.pgpy.PGPMessage.from_blob", return_value=Mock()):
        pgp_instance.privkey = mock_protected_pgp_key[0]
        with pytest.raises(ValueError, match="Passphrase required for protected private key."):
            pgp_instance.decrypt_file(
                input_file="abfss://test/input.txt.pgp",
                output_path="abfss://test/output"
            )

def test_decrypt_file_not_found(pgp_instance, mock_filesystem):
    """Test decryption raises FileNotFoundError for missing file."""
    mock_filesystem.exists.return_value = False
    with pytest.raises(FileNotFoundError, match="Input file not found: abfss://test/input.txt.pgp"):
        pgp_instance.decrypt_file(
            input_file="abfss://test/input.txt.pgp",
            output_path="abfss://test/output"
        )

def test_decrypt_file_io_error(pgp_instance, mock_filesystem, mock_pgp_key):
    """Test decryption raises IOError for file operation failure."""
    with patch("pgp.pgpy.PGPKey.from_blob", return_value=mock_pgp_key), \
         patch("pgp.pgpy.PGPMessage.from_blob", side_effect=IOError("File error")):
        pgp_instance.privkey = mock_pgp_key[0]
        with pytest.raises(IOError, match="Failed to decrypt file: File error"):
            pgp_instance.decrypt_file(
                input_file="abfss://test/input.txt.pgp",
                output_path="abfss://test/output"
            )
