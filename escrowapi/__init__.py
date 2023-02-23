from escrowapi.escrow_api import EscrowAPI, EscrowAPIStatic

eapi = EscrowAPI("hi")
print(eapi)
EscrowAPIStatic.set_comp("waterbottle")

eapi.__comp = "huh whuh"
print(eapi.__comp)

# EscrowAPIStatic.__comp__ = "huh whuh"