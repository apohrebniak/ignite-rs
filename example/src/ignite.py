import pyignite

client = pyignite.Client()
client.connect('localhost', 10800)
client.create_cache('my cache')

                         \                       02 00   ................
0060   00 00 01 65 01 00 65 00 00 00 00 00 00 00 00 f4   ...e..e........ô
0070   01 00 00 00 04 00 00 09 0d 00 00 00 6d 79 5f 6e   ............my_n
0080   65 77 5f 63 61 63 68 65 21 00 04 00 00 00 00 00   ew_cache!.......
0090   00 00 01 00 00 00 01 00 00 08 00 02 00 00 00 00   ................
00a0   00 00 00 00 00 00 00 00 00 00 00 01 00 00 00 00   ................
00b0   00 00 00 00 00 00 00 00 00 00 00 10 27 00 00 00   ............'...
00c0   00 00 00 00 ff ff ff ff 65 02 00 00 00 00 00 00   ....ÿÿÿÿe.......
00d0   00 00 00 00 00                                    .....

