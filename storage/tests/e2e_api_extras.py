"""Part 5 additions: multipart upload, search by date and tags, cluster status, web UI."""
import hashlib
import os
from datetime import date, timedelta

import requests
from common import *  # noqa: F401,F403
from common import META, MiB, RUN, check, delete, finish, get, sha, wait_cluster, wipe_bucket

B = f"extra-{RUN}"
wait_cluster(4)
requests.put(f"{META}/{B}")

# ---- multipart upload
parts = [os.urandom(5 * MiB), os.urandom(5 * MiB), os.urandom(3 * MiB + 123)]
whole = b"".join(parts)
r = requests.post(f"{META}/{B}/big/video.bin", params={"uploads": ""}, headers={"x-amz-meta-owner": "carol", "Content-Type": "video/mp4"})
uid = r.json()["uploadId"]
check("initiate multipart upload -> uploadId", r.status_code == 200 and len(uid) == 32, r.text)
etags = []
for i, p in enumerate(parts, 1):
    pr = requests.put(f"{META}/{B}/big/video.bin", params={"uploadId": uid, "partNumber": i}, data=p)
    etags.append(pr.json().get("etag"))
check("3 parts uploaded, each with its own ETag", pr.status_code == 200 and all(etags) and etags[0] == sha(parts[0]), str(etags))
check("while the upload is open its parts are invisible in listings", requests.get(f"{META}/{B}").json()["contents"] == [])
r = requests.post(f"{META}/{B}/big/video.bin", params={"uploadId": uid})
check("complete -> one object with a multipart ETag", r.status_code == 200 and r.json()["etag"].endswith("-3") and r.json()["size"] == len(whole), r.text)
g = get(B, "big/video.bin")
check("download returns the concatenated parts byte for byte", g.status_code == 200 and g.content == whole)
check("content type and metadata kept", g.headers["content-type"] == "video/mp4" and g.headers.get("x-amz-meta-owner") == "carol")
rg = get(B, "big/video.bin", headers={"Range": f"bytes={5 * MiB - 10}-{5 * MiB + 9}"})
check("byte range across a part boundary", rg.status_code == 206 and rg.content == whole[5 * MiB - 10:5 * MiB + 10])
keys = [c["key"] for c in requests.get(f"{META}/{B}").json()["contents"]]
check("listing shows only the final object (no hidden parts)", keys == ["big/video.bin"], str(keys))
# abort frees the parts
uid2 = requests.post(f"{META}/{B}/aborted.bin", params={"uploads": ""}).json()["uploadId"]
p2 = os.urandom(MiB)
requests.put(f"{META}/{B}/aborted.bin", params={"uploadId": uid2, "partNumber": 1}, data=p2)
h = sha(p2)
check("a part's chunk exists while the upload is open", requests.get(f"{META}/admin/chunks/{h}").status_code == 200)
check("abort -> 204", requests.delete(f"{META}/{B}/aborted.bin", params={"uploadId": uid2}).status_code == 204)
check("abort frees the chunks of the parts", requests.get(f"{META}/admin/chunks/{h}").status_code == 404)
check("unknown upload id -> 404", requests.post(f"{META}/{B}/x", params={"uploadId": "nope"}).status_code == 404)

# ---- search by date and tags
requests.put(f"{META}/{B}/tagged.txt", data=b"hello tags " * 50, headers={"x-amz-meta-project": "midterm", "x-amz-meta-team": "alpha"})
today, tomorrow, yesterday = date.today(), date.today() + timedelta(days=1), date.today() - timedelta(days=1)
def search(**p):
    return [x["key"] for x in requests.get(f"{META}/search", params={"bucket": B, **p}).json()["results"]]
check("search: created after yesterday finds the objects", "tagged.txt" in search(created_after=str(yesterday)))
check("search: created after tomorrow finds nothing", search(created_after=str(tomorrow)) == [])
check("search: created before yesterday finds nothing", search(created_before=str(yesterday)) == [])
check("search: created before tomorrow finds them", "tagged.txt" in search(created_before=str(tomorrow)))
check("search: user-defined tag tag.project=midterm", search(**{"tag.project": "midterm"}) == ["tagged.txt"])
check("search: two tags must both match", search(**{"tag.project": "midterm", "tag.team": "beta"}) == [])
check("search: key prefix + size range", search(prefix="big/", min_size=10 * MiB) == ["big/video.bin"])
check("search: an invalid date is rejected (400)", requests.get(f"{META}/search", params={"created_after": "yesterday"}).status_code == 400)

# ---- cluster status and UI
cs = requests.get(f"{META}/cluster/status").json()
check("cluster status: 4 nodes with health, free space and chunk distribution",
      len(cs["nodes"]) >= 4 and all({"alive", "free_bytes", "pieces", "stored_bytes"} <= set(n) for n in cs["nodes"]))
check("cluster status: storage figures, repairs and policy", {"storage", "recent_repairs", "config", "under_protected_chunks"} <= set(cs))
ui = requests.get(f"{META}/ui")
check("web UI is served at /ui (React)", ui.status_code == 200 and "react" in ui.text.lower() and "Distributed Object Storage" in ui.text)

wipe_bucket(B)
requests.post(f"{META}/admin/gc")
finish()
