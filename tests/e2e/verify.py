"""verify.py seed|check --addr host:port --n N [--size 200] [--conc 64]"""
import argparse,asyncio,time
from nanocached import NanocachedClient
ap=argparse.ArgumentParser(); ap.add_argument("mode"); ap.add_argument("--addr",required=True); ap.add_argument("--n",type=int,default=20000)
ap.add_argument("--size",type=int,default=200); ap.add_argument("--conc",type=int,default=64); ap.add_argument("--start",type=int,default=0)
a=ap.parse_args(); h,p=a.addr.split(","); h,p=h.rsplit(":",1) if False else (a.addr.split(",")[0].rsplit(":",1))
addrs=[(x.rsplit(":",1)[0], int(x.rsplit(":",1)[1])) for x in a.addr.split(",")]
def val(k): return (f"{k}|1|"+"x"*a.size).encode()[:max(a.size,10)]
async def main():
    t0=time.time(); c=await NanocachedClient.connect(addrs); sem=asyncio.Semaphore(a.conc)
    ok=miss=corrupt=err=0
    async def one(i):
        nonlocal ok,miss,corrupt,err
        k=f"k{a.start+i}"
        async with sem:
            try:
                if a.mode=="seed": await c.set(k,val(k)); ok+=1
                else:
                    v=await c.get_bytes(k)
                    if v is None: miss+=1
                    elif v==val(k): ok+=1
                    else: corrupt+=1
            except Exception: err+=1
    await asyncio.gather(*[one(i) for i in range(a.n)])
    st=c.stats(); await c.close()
    import json; print(json.dumps(dict(mode=a.mode,n=a.n,secs=round(time.time()-t0,1),R=c.replication,ok=ok,miss=miss,corrupt=corrupt,err=err,samples=[],stats=str(st))))
asyncio.run(main())
