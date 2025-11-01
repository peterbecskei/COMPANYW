from hashids import Hashids

# CompanyWall might use a custom salt, but let's pick one to demonstrate
hashids = Hashids(salt="companywall.hu", min_length=6, alphabet="abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789")

for n in range(138270, 138276):
    print(n, "→", "MMO" + hashids.encode(n))
