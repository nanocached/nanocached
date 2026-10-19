"""fake_server.py <mode> <listen-port> [target host:port] [seconds]

Small TCP fixtures the error/lifecycle verifications put in front of (or in
place of) a real node:

  silent       accept, read, never answer         -> the client's read/request timeout
  close_mid    accept, read the first request, close without a byte of reply
  accept_close accept and close at once; append a timestamp per connection to
               /out/accepts.log                    -> reconnect storm / backoff
  delay        relay to <target>, delaying the target->client direction by
               <seconds> (default 2)               -> a request stays in flight
  refuse_after relay to <target> but cut every connection <seconds> after it opened

Every wait here is bounded by the container's own lifetime (the run script
removes it); nothing waits for a peer forever on its own behalf.
"""
import asyncio
import sys
import time

mode, port = sys.argv[1], int(sys.argv[2])
target = sys.argv[3] if len(sys.argv) > 3 and ":" in sys.argv[3] else None
secs = float(sys.argv[-1]) if sys.argv[-1].replace(".", "", 1).isdigit() else 2.0


async def pipe(reader, writer, delay=0.0):
    try:
        while True:
            data = await reader.read(65536)
            if not data:
                break
            if delay:
                await asyncio.sleep(delay)
            writer.write(data)
            await writer.drain()
    except (ConnectionError, asyncio.CancelledError):
        pass
    finally:
        try:
            writer.close()
        except Exception:
            pass


async def handle(reader, writer):
    if mode == "silent":
        try:
            while await reader.read(65536):
                pass
        finally:
            writer.close()
    elif mode == "close_mid":
        await reader.read(65536)
        writer.close()
    elif mode == "accept_close":
        with open("/out/accepts.log", "a") as f:
            f.write(f"{time.time():.3f}\n")
        writer.close()
    else:
        host, tport = target.rsplit(":", 1)
        up_r, up_w = await asyncio.open_connection(host, int(tport))
        delay = secs if mode == "delay" else 0.0
        tasks = [asyncio.create_task(pipe(reader, up_w)), asyncio.create_task(pipe(up_r, writer, delay))]
        if mode == "refuse_after":
            await asyncio.sleep(secs)
            for t in tasks:
                t.cancel()
            writer.close()
            up_w.close()
        else:
            await asyncio.wait(tasks)


async def main():
    server = await asyncio.start_server(handle, "0.0.0.0", port)
    async with server:
        await server.serve_forever()


asyncio.run(main())
