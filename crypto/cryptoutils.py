from cryptography.hazmat.primitives.asymmetric import rsa
from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives.asymmetric import padding
from cryptography.exceptions import InvalidSignature
from cryptography.fernet import Fernet

import pickle


def to_bytes(object) -> bytes:
    bytes = pickle.dumps(object)
    return bytes


def from_bytes(bytes):
    object = pickle.loads(bytes)
    return object


def generate_private_public_key_pair(public_exponent=65537, key_size=2048):
    """
    Generates a public private key pair
    :return:
    """
    private_key = rsa.generate_private_key(
        public_exponent=public_exponent,
        key_size=key_size,
    )
    public_key = private_key.public_key()
    return private_key, public_key


def generate_symmetric_key():
    """
    Generates a symmetric key
    """
    sym_key = Fernet.generate_key()
    return sym_key


def get_symmetric_key_from_bytes(key_bytes):
    """
    Generates a Fernet key from bytes
    """
    sym_key = Fernet(key_bytes)
    return sym_key


def encrypt_data_with_public_key(data, public_key):
    """
    Given data and a public key, it encrypts the data
    :return:
    """
    # Even if data is already bytes, we find our own byte representation for compatibility (pickle)
    data = to_bytes(data)
    ciphertext = public_key.encrypt(
        data,
        padding.OAEP(
            mgf=padding.MGF1(algorithm=hashes.SHA256()),
            algorithm=hashes.SHA256(),
            label=None
        )
    )
    return ciphertext


def decrypt_data_with_private_key(ciphertext, private_key):
    """
    Given data signed with DS's public key, this function decrypts that data with DS's private key
    :param data:
    :return:
    """
    plaintext = private_key.decrypt(
        ciphertext,
        padding.OAEP(
            mgf=padding.MGF1(algorithm=hashes.SHA256()),
            algorithm=hashes.SHA256(),
            label=None
        )
    )
    return from_bytes(plaintext)


def encrypt_data_with_symmetric_key(data, key):
    """
    Given data and a symmetric key, encrypt the data with the symmetric key
    :return:
    """
    # Even if data is already bytes, we find our own byte representation for compatibility (pickle)
    ciphertext = None
    try:
        data = to_bytes(data)
        f = Fernet(key)
        ciphertext = f.encrypt(data)
    except Exception as e:
        print("Encryption failed:", str(e))
        ciphertext = None
    finally:
        return ciphertext


def decrypt_data_with_symmetric_key(ciphertext, key):
    """
    Given a ciphertext and a symmetric key in bytes, decrypt the data in-memory
    :return:
    """
    data = None
    try:
        f = Fernet(key)
        data = f.decrypt(ciphertext)
    except Exception as e:
        print("Decryption failed:", str(e))
        data = None
    finally:
        return data
    # return from_bytes(data)


def sign_data(data, private_key):
    """
    Signs a collection of bytes
    :return:
    """
    # Even if data is already bytes, we find our own byte representation for compatibility (pickle)
    data = to_bytes(data)
    signature = private_key.sign(
        data,
        padding.PSS(
            mgf=padding.MGF1(hashes.SHA256()),
            salt_length=padding.PSS.MAX_LENGTH
        ),
        hashes.SHA256()
    )
    return signature


def verify(data, signature, public_key):
    """
    Verifies if a message (data) was signed with the corresponding public_key's private key
    :return:
    """
    # We ensure data is represented in bytes the way we want
    data = to_bytes(data)
    try:
        public_key.verify(
            signature,
            data,
            padding.PSS(
                mgf=padding.MGF1(hashes.SHA256()),
                salt_length=padding.PSS.MAX_LENGTH
            ),
            hashes.SHA256()
        )
    except InvalidSignature:
        return False
    return True


if __name__ == "__main__":
    print("Crypto Utils")

    # Basic usage of private-public key pair (RSA)
    private_key, public_key = generate_private_public_key_pair()

    data = b'example data to encrypt'

    # encryption decryption
    ciph = encrypt_data_with_public_key(data, public_key)

    print("encrypting data with public key: " + str(ciph))

    data2 = decrypt_data_with_private_key(ciph, private_key)

    print("decrypting data with public key: " + str(data2))

    assert(data == data2)

    # sign and verify

    signature = sign_data(data, private_key)

    print("signature: " + str(signature))

    verifies = verify(data, signature, public_key)
    print("Should be true: " + str(verifies))
    verifies = verify(b'diff data', signature, public_key)
    print("Should be false: " + str(verifies))

    # Basic usage of symmetric key using Fernet high-level API
    key = Fernet.generate_key()

    data = b'example data to encrypt with symmetric'

    f = Fernet(key)
    ciph = f.encrypt(data)

    print("ciph symmetric: " + str(ciph))

    decrypted_data = f.decrypt(ciph)

    print("decrypted data symmetric: " + str(decrypted_data))

    dummy = decrypt_data_with_symmetric_key(ciphertext=ciph, key="dummy")
    print("decrypted data with dummy key:", str(dummy))

    # testing from and to bytes
    a = [1, 2, 3, 4]

    print("a: " + str(a))

    a_bytes = to_bytes(a)

    print("a in bytes: " + str(a_bytes))

    a_recovered_from_bytes = from_bytes(a_bytes)

    print("a recovered from bytes: " + str(a_recovered_from_bytes))


