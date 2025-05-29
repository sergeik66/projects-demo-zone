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


Generating PGP key...
Key generated: <class 'pgpy.pgp.PGPKey'>
Protecting private key with passphrase...
Failed to protect private key: <class 'NoneType'>
---------------------------------------------------------------------------
TypeError                                 Traceback (most recent call last)
Cell In[76], line 18
     16 try:
     17     print("Protecting private key with passphrase...")
---> 18     key.protect(passphrase, pgpy.constants.SymmetricKeyAlgorithm.AES256, None)
     19     print("Private key protected with passphrase")
     20 except Exception as e:

File ~/cluster-env/clonedenv/lib/python3.11/site-packages/pgpy/pgp.py:1761, in PGPKey.protect(self, passphrase, enc_alg, hash_alg)
   1758     return
   1760 for sk in itertools.chain([self], self.subkeys.values()):
-> 1761     sk._key.protect(passphrase, enc_alg, hash_alg)
   1763 del passphrase

File ~/cluster-env/clonedenv/lib/python3.11/site-packages/pgpy/packet/packets.py:936, in PrivKeyV4.protect(self, passphrase, enc_alg, hash_alg)
    935 def protect(self, passphrase, enc_alg, hash_alg):
--> 936     self.keymaterial.encrypt_keyblob(passphrase, enc_alg, hash_alg)
    937     del passphrase
    938     self.update_hlen()

File ~/cluster-env/clonedenv/lib/python3.11/site-packages/pgpy/packet/fields.py:1216, in PrivKey.encrypt_keyblob(self, passphrase, enc_alg, hash_alg)
   1214 self.s2k.specifier = String2KeyType.Iterated
   1215 self.s2k.iv = enc_alg.gen_iv()
-> 1216 self.s2k.halg = hash_alg
   1217 self.s2k.salt = bytearray(os.urandom(8))
   1218 self.s2k.count = hash_alg.tuned_count

File ~/cluster-env/clonedenv/lib/python3.11/site-packages/pgpy/decorators.py:47, in sdmethod.<locals>.wrapper(obj, *args, **kwargs)
     46 def wrapper(obj, *args, **kwargs):
---> 47     return sd.dispatch(args[0].__class__)(obj, *args, **kwargs)

File ~/cluster-env/clonedenv/lib/python3.11/site-packages/pgpy/decorators.py:59, in sdproperty.<locals>.defset(obj, val)
     58 def defset(obj, val):  # pragma: no cover
---> 59     raise TypeError(str(val.__class__))

TypeError: <class 'NoneType'>
