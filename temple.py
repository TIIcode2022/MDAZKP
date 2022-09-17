import hashlib

print(hashlib.sha256('a'.encode('utf-8')).hexdigest())