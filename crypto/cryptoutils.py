from cryptography.hazmat.primitives.asymmetric import rsa
from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives.asymmetric import padding
from cryptography.exceptions import InvalidSignature


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


def encrypt_data_with_public_key(data, public_key):
    """
    Given data and a public key, it encrypts the data
    :return:
    """
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
    return plaintext


def encrypt_data_with_symmetric_key():
    """

    :return:
    """
    return


def sign_data(data: bytes, private_key):
    """
    Signs a collection of bytes
    :return:
    """
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
    try:
        public_key.verify(
            signature,
            data,
            padding.PSS(
                mgf=padding.MGF1(hashes.SHA256()),
                salt_length=padding.PSS.MAX_LENGTH
            ),
            hashes.SHA3_256()
        )
    except InvalidSignature:
        return False
    return True


if __name__ == "__main__":
    print("Crypto Utils")


