import pgpy

# Generate a new RSA key pair
try:
    print("Generating PGP key...")
    key = pgpy.PGPKey.new(pgpy.constants.PubKeyAlgorithm.RSAEncryptOrSign, 2048)
    if key is None:
        raise ValueError("Failed to generate PGP key: key is None")
    print(f"Key generated: {type(key)}")
except Exception as e:
    print(f"Error generating PGP key: {str(e)}")
    raise

# Protect the private key with a passphrase
passphrase = "my-passphrase"
try:
    print("Protecting private key with passphrase...")
    key.protect(passphrase, pgpy.constants.SymmetricKeyAlgorithm.AES256, None)
    print("Private key protected with passphrase")
except Exception as e:
    print(f"Failed to protect private key: {str(e)}")
    raise

# Extract the private key as ASCII-armored string
try:
    private_key = str(key)
    print("Private key extracted successfully")
except Exception as e:
    print(f"Error extracting private key: {str(e)}")
    raise
