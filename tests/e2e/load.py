"""load.py --addr host:port --conns N --duration S --keyspace K --value-size B --get-ratio F --label L [--think S] [--reconnect-on-error] [--hedge S]"""
import argparse,asyncio,collections,json,random,time
from nanocached import NanocachedClient
ap=argparse.ArgumentParser()
ap.add_argument("--addr",required=True); ap.add_argument("--conns",type=int,default=8); ap.add_argument("--duration",type=float,default=30)
ap.add_argument("--keyspace",type=int,default=10000); ap.add_argument("--value-size",type=int,default=200); ap.add_argument("--get-ratio",type=float,default=0.7)
ap.add_argument("--think",type=float,default=0.0); ap.add_argument("--reconnect-on-error",action="store_true"); ap.add_argument("--report-every",type=float,default=30)
ap.add_argument("--label",default="load"); ap.add_argument("--hedge",type=float,default=0.0); ap.add_argument("--via-proxy",action="store_true")
a=ap.parse_args(); addrs=[(x.rsplit(":",1)[0], int(x.rsplit(":",1)[1])) for x in a.addr.split(",")]
val=("x"*a.value_size).encode()
res=dict(ops=0,get=0,set=0,miss=0,corrupt=0,err_total=0,errors=collections.Counter(),lat=[],samples=[],sdk_stats=[])
async def worker(idx):
    kw={}
    if a.hedge>0: kw["read_hedge_after"]=a.hedge
    if a.via_proxy: kw["via_proxy"]=True
    c=await NanocachedClient.connect(addrs, **kw)
    try:
        end=time.time()+a.duration
        while time.time()<end:
            k=f"k{random.randrange(a.keyspace)}"
            try:
                if random.random()<a.get_ratio:
                    v=await c.get_bytes(k); res["get"]+=1
                    if v is None: res["miss"]+=1
                else:
                    await c.set(k,val); res["set"]+=1
                res["ops"]+=1
            except Exception as e:
                res["err_total"]+=1; res["errors"][type(e).__name__]+=1
                if a.reconnect_on_error:
                    try: await c.close()
                    except Exception: pass
                    c=await NanocachedClient.connect(addrs, **kw)
            if a.think>0: await asyncio.sleep(a.think)
    finally:
        try: res["sdk_stats"].append(str(c.stats()))
        except Exception: pass
        await c.close()
async def reporter():
    last=0; t0=time.time()
    while True:
        await asyncio.sleep(a.report_every)
        now=res["ops"]; print(json.dumps(dict(t=int(time.time()-t0), ops_s=int((now-last)/a.report_every), p50_ms=0, p99_ms=0, err=res["err_total"], miss=res["miss"], corrupt=res["corrupt"], stale=0)), flush=True)
        last=now
async def main():
    rep=asyncio.ensure_future(reporter())
    await asyncio.gather(*[worker(i) for i in range(a.conns)])
    rep.cancel()
    print(json.dumps(dict(label=a.label, ops=res["ops"], ops_s=int(res["ops"]/a.duration), get=res["get"], set=res["set"], miss=res["miss"], corrupt=res["corrupt"], err_total=res["err_total"], errors=dict(res["errors"]), samples=[], sdk_stats=res["sdk_stats"][:1])))
asyncio.run(main())
