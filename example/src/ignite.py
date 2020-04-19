import pyignite

client = pyignite.Client()
client.connect('localhost', 10800)

# 0000   00 00 00 00 00 00 00 00 00 00 00 00 08 00 45 00
# 0010   00 40 31 df 40 00 40 06 0a d7 7f 00 00 01 7f 00
# 0020   00 01 ae ee 2a 30 62 62 eb 08 1a ec 57 c4 80 18
# 0030   02 00 fe 34 00 00 01 01 08 0a 3f 20 51 26 3f 20
# 0040   51 26 08 00 00 00 01 01 00 02 00 00 00 02
