"""S3-like API: buckets, put/get/head/delete, overwrite, ranges, metadata, listing, search."""
import os

import requests
from common import *  # noqa: F401,F403
from common import META, MiB, RUN, check, delete, finish, get, put, sha, wait_cluster, wipe_bucket

B = f"api-{RUN}"
wait_cluster(4)

# buckets
check("create bucket -> 201", requests.put(f"{META}/{B}").status_code == 201)
check("creating it again -> 409", requests.put(f"{META}/{B}").status_code == 409)
check("invalid bucket name -> 400", requests.put(f"{META}/Bad_Name").status_code == 400)
check("reserved bucket name -> 400", requests.put(f"{META}/admin").status_code == 400)
check("HEAD bucket -> 200", requests.head(f"{META}/{B}").status_code == 200)
check("bucket appears in the bucket list", B in [b["name"] for b in requests.get(f"{META}/").json()["buckets"]])
check("object in a missing bucket -> 404", put("no-such-bucket", "x", b"data").status_code == 404)

# put / get / head, default durability (3 replicas)
data = os.urandom(300 * 1024)
r = put(B, "docs/report.pdf", data, durability="replication", copies=3)
body = r.json()
check("PUT object -> 200 with ETag", r.status_code == 200 and r.headers["ETag"].strip('"') == sha(data), r.text)
check("durability recorded", body["durability"].startswith("replication N=3"), str(body))
g = get(B, "docs/report.pdf")
check("GET returns identical bytes", g.status_code == 200 and g.content == data)
check("GET carries ETag and Content-Length", g.headers["ETag"].strip('"') == sha(data) and int(g.headers["Content-Length"]) == len(data))
h = requests.head(f"{META}/{B}/docs/report.pdf")
check("HEAD -> size and ETag, no body", h.status_code == 200 and h.headers["Content-Length"] == str(len(data)) and not h.content)
check("GET of a missing key -> 404", get(B, "nope").status_code == 404)

# content type and user metadata
r = requests.put(f"{META}/{B}/img/cat.png", data=os.urandom(5000),
                 headers={"Content-Type": "image/png", "x-amz-meta-owner": "alice", "x-amz-meta-project": "midterm"})
g = get(B, "img/cat.png")
check("content type is preserved", g.headers["content-type"] == "image/png")
check("user metadata is returned as x-amz-meta-*", g.headers.get("x-amz-meta-owner") == "alice" and g.headers.get("x-amz-meta-project") == "midterm")

# range reads (single chunk and across chunk boundaries)
big = os.urandom(10 * MiB + 777)
put(B, "big/blob.bin", big, chunk_size=4 * MiB)
for rng, (a, b) in {"bytes=0-99": (0, 99), "bytes=5-5": (5, 5), f"bytes={4 * MiB - 10}-{4 * MiB + 9}": (4 * MiB - 10, 4 * MiB + 9),
                    f"bytes={9 * MiB}-": (9 * MiB, len(big) - 1), "bytes=-1000": (len(big) - 1000, len(big) - 1)}.items():
    g = get(B, "big/blob.bin", headers={"Range": rng})
    check(f"Range {rng[:28]} -> 206 and exact bytes", g.status_code == 206 and g.content == big[a:b + 1]
          and g.headers["Content-Range"] == f"bytes {a}-{b}/{len(big)}", f"{g.status_code} {g.headers.get('Content-Range')}")
check("unsatisfiable range -> 416", get(B, "big/blob.bin", headers={"Range": f"bytes={len(big) + 10}-"}).status_code == 416)

# overwrite replaces the object
old_chunks = requests.get(f"{META}/admin/manifest/{B}/docs/report.pdf").json()["chunks"]
data2 = os.urandom(1000)
r = put(B, "docs/report.pdf", data2)
check("PUT over an existing key replaces it", r.status_code == 200 and r.json()["replaced_existing"] and get(B, "docs/report.pdf").content == data2)
check("chunks of the replaced version were released", requests.get(f"{META}/admin/chunks/{old_chunks[0]['hash']}").status_code == 404)

# listing
for k in ["a/1.txt", "a/2.txt", "a/sub/3.txt", "b/4.txt", "root.txt"]:
    put(B, k, os.urandom(100 + len(k)))
ls = requests.get(f"{META}/{B}", params={"prefix": "a/"}).json()
check("list with prefix", sorted(c["key"] for c in ls["contents"]) == ["a/1.txt", "a/2.txt", "a/sub/3.txt"], str(ls))
ls = requests.get(f"{META}/{B}", params={"prefix": "a/", "delimiter": "/"}).json()
check("list with delimiter rolls up common prefixes", [c["key"] for c in ls["contents"]] == ["a/1.txt", "a/2.txt"] and ls["common_prefixes"] == ["a/sub/"], str(ls))
ls = requests.get(f"{META}/{B}", params={"delimiter": "/"}).json()
check("top-level listing shows folders and files", ls["common_prefixes"] == ["a/", "b/", "big/", "docs/", "img/"] and [c["key"] for c in ls["contents"]] == ["root.txt"], str(ls))
seen, token, pages = [], "", 0
while True:
    page = requests.get(f"{META}/{B}", params={"max-keys": 3, "continuation-token": token}).json()
    seen += [c["key"] for c in page["contents"]]
    pages += 1
    if not page["is_truncated"]:
        break
    token = page["next_continuation_token"]
allkeys = [c["key"] for c in requests.get(f"{META}/{B}").json()["contents"]]
check("pagination with max-keys / continuation-token returns every key once, in order",
      seen == allkeys and len(seen) == len(set(seen)) and pages == -(-len(allkeys) // 3), f"{pages} pages, {seen}")

# search
res = requests.get(f"{META}/search", params={"q": "report", "bucket": B}).json()
check("search by name text", [r["key"] for r in res["results"]] == ["docs/report.pdf"], str(res))
res = requests.get(f"{META}/search", params={"bucket": B, "min_size": 5 * MiB}).json()
check("search by minimum size", [r["key"] for r in res["results"]] == ["big/blob.bin"], str(res))
res = requests.get(f"{META}/search", params={"bucket": B, "content_type": "image/png"}).json()
check("search by content type", [r["key"] for r in res["results"]] == ["img/cat.png"], str(res))
res = requests.get(f"{META}/search", params={"bucket": B, "meta.owner": "alice"}).json()
check("search by user metadata", [r["key"] for r in res["results"]] == ["img/cat.png"], str(res))
res = requests.get(f"{META}/search", params={"bucket": B, "prefix": "a/", "max_size": 108}).json()
check("search combines prefix and size filters", sorted(r["key"] for r in res["results"]) == ["a/1.txt", "a/2.txt"], str(res))

# delete
check("delete bucket that is not empty -> 409", requests.delete(f"{META}/{B}").status_code == 409)
check("DELETE object -> 204", delete(B, "root.txt").status_code == 204 and get(B, "root.txt").status_code == 404)
check("DELETE of a missing key is idempotent (204)", delete(B, "root.txt").status_code == 204)
wipe_bucket(B)
check("bucket is gone after emptying and deleting it", requests.head(f"{META}/{B}").status_code == 404)

finish()
