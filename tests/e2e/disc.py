import socket,sys
addr=sys.argv[1]; h,p=addr.rsplit(":",1)
s=socket.create_connection((h,int(p)),timeout=5); f=s.makefile("rb"); s.sendall(b"L\n"); line=f.readline()
if not line.startswith(b"N"): print("count=0", line); sys.exit(0)
parts=line.split(); cnt,r=int(parts[1]),int(parts[2])
names=[]
for _ in range(cnt):
    nl,al=map(int,f.readline().split()); n=f.read(nl).decode(); a=f.read(al).decode(); f.readline(); names.append((n,a))
print(f"count={cnt} r={r}", names[:3])
