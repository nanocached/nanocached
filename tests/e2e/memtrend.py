import csv,sys,collections
files=sys.argv[1:]; d=collections.defaultdict(list)
for f in files:
    for r in csv.DictReader(open(f)):
        if r['mem_bytes'].isdigit(): d[r['name']].append((int(r['ts']),int(r['mem_bytes'])))
print(f"{'container':12} {'n':>4} {'span':>6} {'first':>8} {'max':>8} {'last':>8} {'slope/h':>9}")
for name,pts in sorted(d.items()):
    if len(pts)<2: continue
    pts.sort(); ts0,v0=pts[0]; ts1,v1=pts[-1]; span=(ts1-ts0)/60
    mx=max(v for _,v in pts)
    slope=(v1-v0)/max(ts1-ts0,1)*3600
    fmt=lambda b: f"{b/1048576:.1f}M"
    print(f"{name:12} {len(pts):4} {span:5.0f}m {fmt(v0):>8} {fmt(mx):>8} {fmt(v1):>8} {slope/1048576:+8.2f}M")
