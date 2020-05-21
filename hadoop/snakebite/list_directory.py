from snakebite.client import Client
import subprocess

client = Client('localhost', 9000)

print("list content: ")
for direct in client.ls(["/"]):
    print(direct)

print("mkdir")
for creation_direct in client.mkdir(["/test_user/input"], create_parent=True):
    print(creation_direct)
